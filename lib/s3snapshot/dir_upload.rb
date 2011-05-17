require 'fog'
require 's3snapshot/sync_op'
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
      
      prefix_path = timepath(@prefix, Time.now)
      
      files = get_local_files
      
      files.each do |file|
        path = "#{prefix_path}/#{file[@local_dir.length..-1]}"
        
        puts "uploading file #{file} to bucket #{@bucket_name} at path #{path}"
        bucket.files.create(:key =>path, :body => File.read(file))
      end
      
      #Gen the "complete" marker file with the complete epoch time
      complete_file = gen_complete_file
      
      #Upload the complete marker
      bucket.files.create(:key => "#{prefix_path}/#{COMPLETE_MARKER}", :body => File.read(complete_file))
      
      #remove the temp dir
      remove_tmp_dir
      
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
    
    ###
    #Creates a temp dir, writes a complete file and returns the path
    def gen_complete_file
      @tmpdir = Dir.mktmpdir
      
      file_path = "#{@tmpdir}/#{COMPLETE_MARKER}"
      
      File.open(file_path, 'w'){ |f| f.write("#{Time.now.utc.iso8601}")}
      
      return file_path
      
    end
    
    
    def remove_tmp_dir
      FileUtils.remove_entry_secure(@tmpdir, true)
    end
  end
end