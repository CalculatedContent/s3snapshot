require 's3snapshot/time_factory'
require 'dictionary'

module S3snapshot
  #
  #Handles retrieving all current backups and performing operations on them.  This object is stateful, once snapshots are loaded it is cached.
  #Create a new instance or call clear_snapshots to force a reload from aws
  #
  class BackupManager < SyncOp
    
    #Number of seconds in a day
    SECONDS_DAY = 60*60*24
    SECONDS_WEEK = SECONDS_DAY * 7
    
    #Instance variable of snapshots by prefix.  This is cached because loading this information is expensive and slow
    @snapshots = nil
    
    
    def initialize(aws_id, aws_key, bucket_name)
      super(aws_id, aws_key, bucket_name)
    end
    
    ##
    #Get all prefixes in the bucket
    #
    def prefixes
      bucket.files.all(:delimiter => "/").common_prefixes
    end
    
    ##
    # returns a map of snapshots.  The key is the time, the value is a boolean signaling if it's complete
    ##
    def snapshots(prefix)
      
      prefix_snap = get_snapshot(prefix)
      
      unless prefix_snap.nil?
        return prefix_snap
      end
      
      prefix_snap = Dictionary.new
      
      timestamps(prefix).each do |timestamp|
        
        time = Time.parse(timestamp)
        
        prefix_snap[time]  = read_complete?(prefix, time)
        
      end
      
      set_snapshot(prefix, prefix_snap)
      
      prefix_snap
      
    end
    
    ##
    #Get the latest completed backup for the given prefix.
    ## Will return nil if one isn't available  
    def latest(prefix)
      snapshots(prefix).each do  |time, complete|
        if complete
          return time
        end
      end
      
      nil
    end
    
    ##
    #clear the local cached copy of the snapshot
    #
    def clear_snapshots(prefix)
      @snapshots[prefix] = nil
    end
    
    
    
    #Returns true if the backup is complete, false otherwise
    def complete?(prefix, time)
      value = snapshots(prefix)[time]
      value.nil? ? false : value
    end
    
    ##
    # Returns true if the backup exists
    #
    def exists?(prefix, time)
      !snapshots(prefix)[time].nil?
      #      backups = bucket.files.all(:prefix => timepath(prefix, time))
      #      
      #      !backups.nil? && backups.length > 0
    end
    
    
    ##
    #Removes all incomplete backups.  Use wisely, will blitz a backup in progress
    ##
    def clean(prefix)
      snapshots(prefix).each do |time, complete|
        unless complete
          remove(prefix, time)
        end
      end
    end
    
    #
    #Delete all files from a snapshot.  Will remove the complete file first to avoid other clients using the backup
    #
    def remove(prefix, timestamp)
      complete_marker =  bucket.files.get(complete_path(prefix, timestamp))
      
      #Destroy the complete marker.  This prevents other clients from thinking this backup is complete when the s3 bucket is read
      unless complete_marker.nil?
        complete_marker.destroy
      end
      
      files = list_files(prefix, timestamp)
      
      files.each do |file|
        file.destroy
      end
      
      get_snapshot(prefix).delete(timestamp)
      
    end
    
    
    ##
    # Return all files that exist in this backup bucket with the given time
    ##
    def list_files(prefix, time)
      bucket.files.all(:prefix => timepath(prefix, time) )
    end
    
    
    
    ##
    #Perform a rolling delete for the given prefix.  Keeps the "newest" daily backup for the given day, and keeps a backup for the day of the week specified
    # by day of week.  day_of_week follows cron style syntax, 0 = sunday and 6 = saturday
    def roll(prefix, num_days, num_weeks, day_of_week)
      
      start =  TimeFactory.utc_time
      
      start = Time.utc(start.year, start.month, start.day)
      
      clean(prefix)
      
      merge_days(prefix, start)
      
      
      snaps = snapshots(prefix)
      
      #Nothing to do
      if snaps.nil? ||snaps.empty?
        return
      end
      
      newest_time =snaps.keys.last
      
      #Truncate the oldest daily to 00 hours minutes and seconds based on the "newest" completed backup after the merge
      oldest_daily = newest_time - SECONDS_DAY*num_days
      
      oldest_daily = Time.utc(oldest_daily.year, oldest_daily.month, oldest_daily.day)
      
      #Truncate the oldest weekly to 00 hours minutes and seconds
      oldest_weekly = newest_time - SECONDS_WEEK*num_weeks
      
      oldest_weekly = Time.utc(oldest_weekly.year, oldest_weekly.month, oldest_weekly.day)
      
      
      #Now iterate over every day and keep the number of days.  After that only keep the value that's on the number of weeks
      snaps.each do |time, complete|
        
        #We're done, we've fallen into the day range
        if time >= oldest_daily
          break
        end
        
        #Is older than the oldest daily,or not the right day of the week so should be deleted
        if time < oldest_weekly || !same_day(time, day_of_week)
          remove(prefix, time)
        end
        
      end
    end
    
    
    private
    
    
    #Returns true if the backup is complete, false otherwise. Downloads the file so can be slow
    def read_complete?(prefix, time)
      
      found_prefixes = bucket.files.all(:prefix => timepath(prefix, time), :delimiter => COMPLETE_EXTENSION).common_prefixes
      
      
      !found_prefixes.nil? && found_prefixes.length > 0
    end
    
    ##
    # Returnes true if the time occurs on the same day_of_week.  day_of_week follows cron style syntax, 0 = sunday and 6 = saturday
    #
    def same_day(time, day_of_week)
      
      unless day_of_week > -1 && day_of_week < 7
        raise "Invalid day of week. Expected 0-6 but received #{day_of_week}"
      end
      
      time.wday == day_of_week
      
    end
    
    
    ##
    #Iterates over all snapshots and removes any duplicates for a day.  Only keep the latest for a day
    ##
    def merge_days(prefix, start)
      #Use "yesterday" as a starting point
      previous = nil
      
      snapshots(prefix).each do |time, complete|
        #Skip anything that's before the "start" time above
        if time > start || !complete
          next  
        end
        
        #2 backups on the same day, keep the oldest
        if samedate?(previous, time)  
          if(previous.to_i > time.to_i)
            remove(prefix, time)
          else
            remove(prefix, previous)
            previous = time
          end
          
          next
        end
        
        previous = time
        
      end
    end
    
    #returns true if the first and second time occur in the same date (assumes utc time)
    def samedate?(first, second)
      puts "first: #{first};#{first.nil? ? "nil": first.yday}, second #{second};#{second.nil? ? "nil": second.yday}"
      !first.nil? && !second.nil? && first.yday == second.yday
    end
    
    def timestamps(prefix)
      bucket.files.all(:prefix => prefix_string(prefix), :delimiter => "/").common_prefixes
    end
    
    def prefix_string(prefix)
      "#{prefix}/"
    end
    
    
    ##
    # Get a snapshot by prefix
    ##
    def get_snapshot(prefix)
      @snapshots.nil? ? nil : @snapshots[prefix]
    end
    
    ##
    #Set the snapshot into the context
    ##
    def set_snapshot(prefix, snaphash)
      if @snapshots.nil?
        @snapshots = Hash.new
      end
      
      @snapshots[prefix] = snaphash
    end
    
    
  end
end
