require 'thor'
require 's3snapshot/dir_upload'
require 's3snapshot/backup_manager'

module S3snapshot
  class CLI < Thor
    
   
    desc "upload", "upload a directory as a snapshot backup to s3"

    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :directory, :aliases => "-d", :desc => "The directory to upload", :type => :string, :required => true
    method_option :prefix, :aliases => "-p", :desc => "A prefix to prepend to the path before the timestamp.  Useful in cluster to specifiy a node name" , :type => :string,  :required => true
    
    ##
    #Uploads the directory to the s3 bucket with a prefix
    def upload()
      directory = options[:directory]
      puts "You are uploading directory #{directory}"
            
      s3upload = DirUpload.new(options[:awsid], options[:awskey],  options[:bucket],   options[:prefix], directory )
      s3upload.upload
    end
    
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
 
    def list_prefix
      manager = BackupManager.new(options[:awsid], options[:awskey], options[:bucket])

    end
  end
end