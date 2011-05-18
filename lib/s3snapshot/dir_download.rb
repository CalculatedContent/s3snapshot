require 'fog'
require 's3snapshot/sync_op'
require 'time'
require 'fileutils'

module S3snapshot
  class DirDownload < SyncOp
    
    @timestap = nil
    @local_dir = nil
    @prefix = nil
    
    
    def initialize(aws_id, aws_key, bucket_name, prefix, timestamp, local_dir )
      super(aws_id, aws_key, bucket_name)
      @prefix = prefix
      @timestamp = timestamp
      @local_dir = local_dir
    end
    
    def download
      
      
      begin
        start_time = Time.parse(@timestamp)
      rescue Exception => e
        puts "Could not parse timestamp #{@timestamp}"
        raise e
      end
      
      prefix_path = timepath(@prefix, start_time)
      
      #Get all files from this backup
      files = bucket.files.all(:prefix => prefix_path)
      
      #Make the local directory
      FileUtils.mkdir(@local_dir)
      
      files.each do |file|
        destination_path = "#{@local_dir}/#{file.name[prefix_path.length..-1]}"
        
        puts "downloading '#{file.name}' to '#{destination_path}'"
        file.save(:key =>path, :body => File.read(file))
      end
      
      
      puts "Writing complete marker"
      
      #Gen the "complete" marker file with the complete epoch time
      complete_file = gen_complete_file
      
      #Upload the complete marker
      bucket.files.create(:key => complete_path(@prefix, start_time), :body => File.read(complete_file))
      
      #remove the temp dir
      remove_tmp_dir
      
      puts "backup complete!"
    end
    
    
    private
    
    ##
    #Finds all files in a directory and returns them in an array
    ##
    def get_local_files
      files = Array.new
      Dir.glob( File.join(@local_dir, '**', '*') ).each do |file| 
        unless File.directory?(file)
          files << file
        end
      end
      
      return files
    end
    
    
  end
end