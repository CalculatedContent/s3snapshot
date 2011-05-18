require 'fog'
require 's3snapshot/sync_op'
require 's3snapshot/backup_manager'
require 'time'
require 'fileutils'

module S3snapshot
  class DirDownload < SyncOp
    
    @time = nil
    @local_dir = nil
    @prefix = nil
    
    
    def initialize(aws_id, aws_key, bucket_name, prefix, time, local_dir )
      super(aws_id, aws_key, bucket_name)
      @prefix = prefix
      @time = time
      @local_dir = local_dir
    end
    
    def download
      
      prefix_path = timepath(@prefix, @time)
      
      #Check if the backup is complete
      manager = BackupManager.new(@aws_id, @aws_key, @bucket_name)
      
      unless manager.exists?(@prefix, @time)
        $stderr.puts "Backup with prefix '#{@prefix}' and time #{@time.iso8601} does not exist.  Please check the prefix and time"
        return
      end
      
      unless manager.complete?(@prefix, @time)
        $stderr.puts "Backup with prefix '#{@prefix}' and time #{@time.iso8601} is not complete.  The backup is either in progress or never finished.  This snapshot is not safe to restore!"
        return
      end
      
      #Get all files from this backup
      files = bucket.files.all(:prefix => prefix_path)
      
      #Make the local directory
      unless File.directory?(@local_dir)
        FileUtils.mkdir(@local_dir)
      end
      
      files.each do |file|
        destination_path = "#{@local_dir}/#{file.key[prefix_path.length+1..-1]}"
        
        directory = destination_path[0..-File.basename(destination_path).length-1]
        
        #Create the parent directory for the file if it doesn't exist
        unless File.directory?(directory)
          FileUtils.mkdir(directory)
        end
        
        puts "downloading '#{file.key}' to '#{destination_path}'"
        
        #Open the file in read/write and create it if it doesn't exist, then write the content from s3 into it
        File.open(destination_path, File::RDWR|File::CREAT){ |local| local.write(file.body)}
        
      end
      
      
      puts "Writing complete marker"
      
      
      puts "backup complete!"
    end
    
    
    private
    
    
    
    
  end
end