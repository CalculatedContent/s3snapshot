require 'fog'

module S3snapshot
  class SyncOp
    
    #Constant for the file that will be present with the complete timestamp if a directory was successfully backed up
    COMPLETE_FILE = "s3snapshot"
    COMPLETE_EXTENSION = "complete_marker"
    COMPLETE_MARKER = "#{COMPLETE_FILE}.#{COMPLETE_EXTENSION}"
    
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
      #      @bucket ||=  aws.directories.get(@bucket_name)
      aws.directories.get(@bucket_name)
    end
    
    #Generate the time path.  If a prefix is specified the format is <prefix>/<timestamp> otherwise it is timestamp.  All timestamps are in iso 8601 format and in 
    # the UTC time zone
    def timepath(prefix, time)
     "#{prefix}/#{time.utc.iso8601}"
    end
    
    
    #
    #The path to the complete file with the given prefix and time
    #
    def complete_path(prefix, time)
      "#{complete_prefix(prefix, time)}#{COMPLETE_MARKER}"
    end
    
    ##
    #Constructs a prefix in the format of [prefix]/[iso time]/complete_file
    def complete_prefix(prefix, time)
      "#{timepath(prefix, time)}/#{COMPLETE_FILE}"
    end
    
    
  end
end