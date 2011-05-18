require 'thor'
require 's3snapshot/dir_upload'
require 's3snapshot/dir_download'
require 's3snapshot/backup_manager'

module S3snapshot
  class CLI < Thor
    
    
    desc "backup", "upload a directory as a snapshot backup to s3"
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :directory, :aliases => "-d", :desc => "The directory to upload", :type => :string, :required => true
    method_option :prefix, :aliases => "-p", :desc => "A prefix to prepend to the path before the timestamp.  Useful in cluster to specifiy a node name, or a node+directory scheme.  Prefix strategies can be mixed in a bucket, they must just be unique." , :type => :string,  :required => true
    
    ##
    #Uploads the directory to the s3 bucket with a prefix
    def backup
      directory = options[:directory]
      puts "You are uploading directory #{directory}"
      
      s3upload = DirUpload.new(options[:awsid], options[:awskey],  options[:bucket],   options[:prefix], directory )
      s3upload.upload
    end
    
    
    desc "prefixes", "list all prefixes in an s3 bucket"
    
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    
    def prefixes
      manager = BackupManager.new(options[:awsid], options[:awskey], options[:bucket])
      
      puts "Found the following prefixes\n\n"
      
      manager.prefixes.each do |prefix|
        puts prefix[0..-2]
      end
      
      puts "\n"
    end
    
    
    desc "snapshots", "list all snapshots for a prefix in an s3 bucket"
    
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :prefix, :aliases => "-p", :desc => "The prefix to prepend to before searching for snapshots" , :type => :string,  :required => true
    
    
    def snapshots
      manager = BackupManager.new(options[:awsid], options[:awskey], options[:bucket])
      
      snap_map = manager.snapshots(options[:prefix])
      
      puts "Found the following timestamps from prefix #{options[:prefix]}\n\n"
      
      snap_map.each do |key, value|
        result = value ? "complete" : "unknown"
        
        puts "Time: #{key.iso8601}, Status: #{result}"
      end
      
      puts "\n"
      
    end
    
    desc "restore", "restore all files from a snapshot to a directory"
    
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :prefix, :aliases => "-p", :desc => "The prefix to prepend to before searching for snapshots" , :type => :string,  :required => true
    method_option :time, :aliases => "-t", :desc => "The timestamp to restore" , :type => :string,  :required => true
    method_option :dest, :aliases => "-d", :desc => "The desitnation directory for downloaded files" , :type => :string,  :required => true
    
    
    def restore
      time = Time.parse(options[:time])
      download = DirDownload.new(options[:awsid], options[:awskey], options[:bucket], options[:prefix], time, options[:dest])
      download.download
    end
  end
end