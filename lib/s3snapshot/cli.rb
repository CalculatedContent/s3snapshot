require 'thor'

module S3snapshot
  class CLI < Thor
    
    desc "upload", "upload a directory as a snapshot backup to s3"

    method_option :awsid, :aliases => "-ai", :desc => "The aws id"
    method_option :awskey,:aliases => "-ak", :desc => "The aws secret key"
    method_option :awsbucket, :aliases => "-b", :desc => "The aws bucket to use"
    method_option :prefix, :aliases => "-p", :desc => "An optional prefix to prepend to the path before the timestamp.  Useful in cluster to specifiy a node name" 
    
    ##
    #Uploads the directory to the s3 bucket with a prefix
    def upload(directory)
      puts "You are uploading director #{directory}"
#      s3upload = DirUpload.new(directory, options[:awsbucket], options[:prefix], options[:awsid], options[:awskey])
#      s3upload.upload
    end
    
  end
end