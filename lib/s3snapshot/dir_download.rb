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
      files = manager.list_files(@prefix, @time)
      
      #Make the local directory
      unless File.directory?(@local_dir)
        FileUtils.mkdir(@local_dir)
      end
      
      prefix_path = timepath(@prefix, @time)
      
      
      files.each do |remotefile|
        #We have to reload state from s3.  Otherwise we can't download when the restore process takes a while
        destination_path = "#{@local_dir}/#{remotefile.key[prefix_path.length+1..-1]}"
        
        directory = destination_path[0..-File.basename(destination_path).length-1]
        
        #Create the parent directory for the file if it doesn't exist
        unless File.directory?(directory)
          FileUtils.mkdir(directory)
        end
        
        puts "downloading '#{remotefile.key}' to '#{destination_path}'"
        
        File.open(destination_path, File::RDWR|File::CREAT) do |file|
          bucket.files.get(remotefile.key) do |chunk, remaining_bytes, total_bytes|
            file.write(chunk)
            percent = ((1-remaining_bytes.to_f/total_bytes.to_f)*100).round
            puts "#{percent}% complete"
          end
        end
        
      end
      
      
      
      puts "Writing complete marker"
      
      
      puts "backup complete!"
    end
    
    
    private
    
    
    
    
  end
end