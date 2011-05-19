require 'thor'
require 's3snapshot/dir_upload'
require 's3snapshot/time_factory'


module S3snapshot
  class TestLoader < Thor
    ##
    #Test data generation
    ##
    
    
    desc "gendata", "upload a directory as a snapshot backup to s3"
    
    method_option :awsid, :aliases => "-i", :desc => "The aws id", :type => :string, :required => true
    method_option :awskey,:aliases => "-k", :desc => "The aws secret key", :type => :string, :required => true
    method_option :bucket, :aliases => "-b", :desc => "The aws bucket to use", :type => :string, :required => true
    method_option :directory, :aliases => "-d", :desc => "The directory to upload", :type => :string, :required => true
    method_option :prefix, :aliases => "-p", :desc => "A prefix to prepend to the path before the timestamp.  Useful in cluster to specifiy a node name, or a node+directory scheme.  Prefix strategies can be mixed in a bucket, they must just be unique." , :type => :string,  :required => true
    method_option :hours, :aliases => "-h", :desc => "The number of hours between backups to generate a timestamp for", :type => :numeric
    method_option :numbackups, :aliases => "-n", :desc=> "The maximum number of iterations to run at 'hours' interval", :type => :numeric
    
    ##
    #Uploads the directory to the s3 bucket with a prefix
    def gendata
      directory = options[:directory]
      puts "You are uploading directory #{directory}"
      
      hours = options[:hours]
      backups = options[:numbackups] 
      
      # subtract off the number of hours * number of backups and convert the hours to seconds to set the start time
      time = Time.now.utc - hours * backups * 3600
      
      for i in (1.. backups)
        
        TimeFactory.set_time (time)
        s3upload = DirUpload.new(options[:awsid], options[:awskey],  options[:bucket],   options[:prefix], directory )
        s3upload.upload
        
        
        
        time += hours * 3600
        
      end
      
      
    end
  end
end

S3snapshot::TestLoader.start