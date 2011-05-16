require 'fog'
require 's3snapshot/sync_op'

module S3snapshot
  class DirUpload < SyncOp
    
    def upload
      
      epoch = time.now.to_i
      
      prefix_path = "#{prefix}/#{epoch}"
      
      files = get_local_files
      
      files.each do |file|
        path = "#{prefix_path}/#{file}"
        puts "uploading file #{file} to bucket #{@aws_bucket} at path #{path}"
        bucket.files.create(:key =>path, :body => File.read(file))
      end
      
      
    end
    
    
    private
    
    ##
    #Finds all files in a directory and returns them in an array
    ##
    def get_local_files
      files = Array.new
      Dir.glob( File.join(@local_dir, '**', '*') ) { |file| files << file }
      return files
    end
    
  end
end
