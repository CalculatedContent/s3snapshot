require 'fog'
require 's3snapshot/sync_op'
require 's3snapshot/time_factory'
require 'time'
require 'fileutils'


module S3snapshot
  class DirUpload < SyncOp
    
    @tmpdir = nil
    @local_dir = nil
    @prefix = nil
    
    def initialize(aws_id, aws_key, bucket_name, prefix, local_dir )
      super(aws_id, aws_key, bucket_name)
      @local_dir = local_dir
      @prefix = prefix

    end
    
    def upload
      
      start_time = TimeFactory.utc_time
      
      prefix_path = timepath(@prefix, start_time)
      
      files = get_local_files
      
      files.each do |file|
        path = "#{prefix_path}/#{file[@local_dir.length+1..-1]}"
        
        puts "uploading '#{file}' to '#{@bucket_name}/#{path}'"
        bucket.files.create(:key =>path, :body => File.read(file))
      end
      
      
      puts "Writing complete marker"
      
       #Upload the complete marker
      bucket.files.create(:key => complete_path(@prefix, start_time), :body => TimeFactory.utc_time.iso8601)
      
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