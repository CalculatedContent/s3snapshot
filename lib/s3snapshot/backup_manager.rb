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
    def remove_incomplete(prefix, time)
      
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