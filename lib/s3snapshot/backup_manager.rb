module S3snapshot
  #
  #handles retrieving all current backups and performing operations on them
  #
  class BackupManager < SyncOp
    
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
    #returns a map of snapshots.  The key is the time, the value is a boolean signaling if it's complete
    ##
    def snapshots(prefix)
      
      map = {}
      
      timestamps(prefix).each do |timestamp|
        
        time = Time.parse(timestamp)
        
        map[time]  = complete?(prefix, time)
      end
      
      map
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
    
    #Delete all files from a snapshot.  Will remove the complete file first
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
    
    
    private
    
    
    
    def timestamps(prefix)
      bucket.files.all(:prefix => prefix_string(prefix), :delimiter => "/").common_prefixes
    end
    
    def prefix_string(prefix)
      "#{prefix}/"
    end
    
    
    
  end
end