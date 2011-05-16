require 'fog'

module S3snapshot
  class SyncOp
    
    @local_dir
    @bucket_name
    @prefix
    @aws_id
    @aws_key
    
    #Our to aws connection
    @aws
    
    #The current bucket
    @bucket
    
    def initialize(local_dir, bucket_name, prefix, aws_id, aws_key)
      @local_dir = local_dir
      @bucket_name = bucket_name
      @prefix = prefix
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
    
  end
end