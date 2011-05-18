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
    def print_prefixes
      puts "Found the following prefixes\n\n"
      
      prefixes.each do |prefix|
        puts prefix[0..-2]
      end
      
      puts "\n"
    end
    
    
    ##
    #Get all prefixes in the bucket
    #
    def print_snapshots(prefix)
      
      prefix_padded = prefix_string(prefix)
      
      puts "Found the following timestamps from prefix #{prefix}\n\n"
      
      timestamps(prefix).each do |timestamp|
        
        result = complete?(prefix, timestamp) ? "complete" : "unknown"
        
        puts "Time: #{timestamp[prefix_padded.length..-2]}, Status: #{result}"
      end
      
      puts "\n"
    end
    
    
    
    
    
    private
    
    
    
    def prefixes
      bucket.files.all(:delimiter => "/").common_prefixes
    end
    
    def timestamps(prefix)
      bucket.files.all(:prefix => prefix_string(prefix), :delimiter => "/").common_prefixes
    end
    
    def prefix_string(prefix)
      "#{prefix}/"
    end
    
    #Construct the base path from the string prefix and iso timestamp,then check for completed file
    def complete?(prefix, timestamp)
      time = Time.parse(timestamp)
            
      complete_prefix = bucket.files.all(:prefix => complete_prefix(prefix, time))
      
      !complete_prefix.nil? && complete_prefix.length > 0
    end
    
  end
end