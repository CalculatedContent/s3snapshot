require 's3snapshot/time_factory'

module S3snapshot
  #
  #handles retrieving all current backups and performing operations on them
  #
  class BackupManager < SyncOp
    
    #Number of seconds in a day
    SECONDS_DAY = 60*60*24
    SECONDS_WEEK = SECONDS_DAY * 7
    
    #Instance variable of snapshot.  This is cached because loading this information is expensive and slow
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
      
      unless @snapshots.nil?
        return @snapshots
      end
      
      @snapshots = {}
      
      timestamps(prefix).each do |timestamp|
        
        time = Time.parse(timestamp)
        
        @snapshots[time]  = complete?(prefix, time)
        
      end
      
      @snapshots
      
    end
    
    
    
    
    
    #Returns true if the backup is complete, false otherwise
    def complete?(prefix, time)
      complete_prefix = bucket.files.all(:prefix => complete_prefix(prefix, time))
      
      !complete_prefix.nil? && complete_prefix.length > 0
    end
    
    ##
    # Returns true if the backup exists
    #
    def exists?(prefix, time)
      
      backups = bucket.files.all(:prefix => timepath(prefix, time))
      
      !backups.nil? && backups.length > 0
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
     
      clean(prefix)
      
      merge_days(prefix, start)
      
      
      snap_list = snapshots(prefix)
      
      #Nothing to do
      if snap_list.nil? || snap_list.empty?
        return
      end
      
      #Truncate the oldest daily to 00 hours minutes and seconds based on the "newest" completed backup after the merge
      oldest_daily = snap_list.first.key - SECONDS_DAY*num_days
      
      oldest_daily = Time.utc(oldest_daily.year, oldest_daily.month, oldest_daily.day)
      
      #Truncate the oldest weekly to 00 hours minutes and seconds
      oldest_weekly = snap_list.first.key - SECONDS_WEEK*num_weeks
      
      oldest_weekly = Time.utc(oldest_weekly.year, oldest_weekly.month, oldest_weekly.day)
      
      
      #Now iterate over every day and keep the number of days.  After that only keep the value that's on the number of weeks
      snapshots(prefix).each do |time, complete|
        if time < oldest_daily && !same_day(time, day_of_week)
          remove(prefix, time)
        end
        
        if time < oldest_weekly
          remove(prefix, time)
        end
        
      end
    end
    
    
    private
    
    
    ##
    # Returnes true if the time occurs on the same day_of_week.  day_of_week follows cron style syntax, 0 = sunday and 6 = saturday
    #
    def same_day(time, day_of_week)
      case day_of_week
        when 0
        return time.sunday?
        when 1
        return time.monday?
        when 2
        return time.tuesday?
        when 3
        return time.wednesday?
        when 4
        return time.thursday?
        when 5
        return time.friday?
        when 6
        return time.saturday?
      else
        raise "Invalid day of week. Expected 0-6 but received #{day_of_week}"
      end
      
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
      !first.nil? && !second.nil? && first.yday == second.yday
    end
    
    def timestamps(prefix)
      bucket.files.all(:prefix => prefix_string(prefix), :delimiter => "/").common_prefixes
    end
    
    def prefix_string(prefix)
      "#{prefix}/"
    end
    
    
    
  end
end
