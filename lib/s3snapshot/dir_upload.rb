require 'fog'
require 's3snapshot/sync_op'
require 's3snapshot/time_factory'
require 'time'
require 'fileutils'
require 'digest/md5'
require 'base64'

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
        file_name = file[@local_dir.length..-1];
        
        #Strip the leading "/" from file paths
        if file_name.length > 0 && file_name[0] == '/'
          file_name = file[1..-1]
        end
        
        path = "#{prefix_path}/#{file_name}"

        # check if file is greater than 5GB 

        two_g_bytes = 1024*1024*1024*1
        fsize = File.size file
        puts "uploading '#{file}' [#{fsize} bytes] to '#{@bucket_name}/#{path}'"
        if fsize > two_gb_bytes
          upload_file_as_multipart(file, path)
        else
          # normal upload
          File.open(file) do |fb|
            bucket.files.create(:key =>path, :body => fb)
          end
          
        end
        
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

    # expects object_to_upload to NOT have a leading /
    def upload_file_as_multipart(object_to_upload, object_key)

      # method based on gist from https://gist.github.com/908875

      puts "Uploading file as multipart: file=#{object_to_upload}"

      # tmp dir to place the split file into
      workdir = "/tmp/work/#{File.basename(object_to_upload)}/"
      FileUtils.mkdir_p(workdir)

      # Assumes we are running on unix with the split command available.
      # Split the file into chunks, max size of 1G, the chunks are 000, 001, etc
      `split -C 1G -a 3 -d #{object_to_upload} #{workdir}`
      puts "Split file into parts in #{workdir}"

      # Map of the file_part => md5
      parts = {}

      # Get the Base64 encoded MD5 of each file
      Dir.entries(workdir).each do |file|
        next if file =~ /\.\./
        next if file =~ /\./

        md5 = Base64.encode64(Digest::MD5.file("#{workdir}/#{file}").digest).chomp!

        full_path = "#{workdir}#{file}"

        parts[full_path] = md5
      end

      ### Now ready to perform the actual upload

      # Initiate the upload and get the uploadid
      multi_part_up = @aws.initiate_multipart_upload(bucket, object_key, { 'x-amz-acl' => 'private' } )
      upload_id = multi_part_up.body["UploadId"]

      # Lists for the threads and tags
      tags = []
      threads = []

      sorted_parts = parts.sort_by do |d|
        d.split('/').last.to_i
      end

      sorted_parts.each_with_index do |entry, idx|
        # Part numbers need to start at 1
        part_number = idx + 1

        # Reload to stop the connection timing out, useful when uploading large chunks
        @aws.reload

        # Create a new thread for each part we are wanting to upload.
        threads << Thread.new(entry) do |e|
          print "DEBUG: Starting on File: #{e[0]} with MD5: #{e[1]} - this is part #{part_number} \n"

          # Pass fog a file object to upload
          File.open(e[0]) do |file_part|

            # The part_number changes each time, as does the file_part, however as they are set outside of the threads being created I *think* they are
            # safe. Really need to dig into the pickaxe threading section some more..
            part_upload = @aws.upload_part(bucket, object_key, upload_id, part_number, file_part, { 'Content-MD5' => e[1] } )

            # You need to make sure the tags array has the tags in the correct order, else the upload won't complete
            tags[idx] = part_upload.headers["ETag"]

            print "#{part_upload.inspect} \n" # This will return when the part has uploaded
          end
        end
      end

      # Make sure all of our threads have finished before we continue
      threads.each do |t|
        begin
          t.join
        rescue Exception => e
          puts "UPLOAD FAILED: #{e.message}"
          # do we need to cancel the upload here?
        end
      end

      # Might want a @aws.reload here...

      completed_upload = @aws.complete_multipart_upload(bucket, object_key, upload_id, tags)

      # clean up the tmp files
      Dir.entries(workdir).each do |file|
        next if file =~ /\.\./
        next if file =~ /\./

        File.delete "#{workdir}/#{file}"
      end
    end
    
    
  end
end