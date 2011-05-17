require 'fog'

module S3snapshot
  class SyncOp
    
    #Constant for the file that will be present with the complete timestamp if a directory was successfully backed up
    COMPLETE_MARKER = "s3snapshot_complete.txt"
    
    @bucket_name
    @aws_id
    @aws_key
    
    #Our to aws connection
    @aws
    
    #The current bucket
    @bucket
    
    def initialize(aws_id, aws_key, bucket_name)
      @bucket_name = bucket_name
      @aws_id = aws_id
      @aws_key = aws_key
    end
    
    ##
    # Return the cached aws connection or create a new one
    ##
    def aws
      @aws ||= Fog::Storage.new(:provider => 'AWS', :aws_access_key_id => @aws_id, :aws_secret_access_key => @aws_key)
    end
    
    ##
    #Get the cached bucket or create the new one
    ##
    def bucket
      @bucket ||=  aws.directories.get(@bucket_name)
    end
    
    #Generate the time path.  If a prefix is specified the format is <prefix>/<timestamp> otherwise it is timestamp.  All timestamps are in iso 8601 format and in 
    # the UTC time zone
    def timepath(prefix, time)
      if prefix.nil?
        return "#{time.utc.iso8601}"
      end
      
      return "#{prefix}/#{time.utc.iso8601}"
      
    end
    
    
    
  end
end