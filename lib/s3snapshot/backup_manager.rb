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
#      dirs = bucket.files.all(:delimiter => "/")
      dirs = bucket.files
      
      dirs.each do |dir|
        puts "Found file #{dir}"
      end
      
    end
    
  end
end