require 'thor'

module S3snapshot
  class CLI < Thor
    
   
    desc "upload", "upload a directory as a snapshot backup to s3"

    method_option :awsid, :default => nil, :aliases => "-ai", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:default => nil, :aliases => "-ak", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :default => nil, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :prefix, :default => nil, :aliases => "-p", :desc => "An optional prefix to prepend to the path before the timestamp.  Useful in cluster to specifiy a node name" , :type => :string
    
    
    ##
    #Uploads the directory to the s3 bucket with a prefix
    def upload(directory)
      puts "You are uploading directory #{directory}"
            
      s3upload = DirUpload.new(directory, options[:bucket],  options[:prefix], options[:awsid], options[:awskey])
      s3upload.upload
    end
    
  end
end