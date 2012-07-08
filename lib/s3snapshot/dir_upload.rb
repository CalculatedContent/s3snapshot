require 'fog'
require 's3snapshot/sync_op'
require 's3snapshot/time_factory'
require 'time'
require 'fileutils'
require 'digest/md5'
require 'base64'
require 'date'
require 'tmpdir'

module S3snapshot
  class DirUpload < SyncOp
    
    @tmpdir = nil
    @local_dir = nil
    @prefix = nil

    MAX_RETRY_COUNT = 5;
    
    def initialize(aws_id, aws_key, bucket_name, prefix, local_dir, tmp_dir = nil )
      super(aws_id, aws_key, bucket_name)
      @local_dir = local_dir
      @prefix = prefix

      @tmpdir = tmp_dir ? tmp_dir : Dir.tmp
      if File.exists? @tmpdir
        puts "Temp directory #{@tmpdir} exists."
      else
        begin
          FileUtils.mkdir_p @tmpdir
          FileUtils.chmod 0777, @tmpdir
        rescue Exception => e
          puts "Unable to create directory #{@tmpdir} due to #{e.message}"
          puts e.backtrace.join("\n")
          exit 5  
        end
      end
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

        split_threshhold = 1024*1024*1024*4
        fsize = File.size file
        puts "uploading '#{file}' [#{fsize} bytes] to '#{@bucket_name}/#{path}'"
        # check if file is greater than 5G 
        if fsize > split_threshhold
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

    ##
    # Produces a hash of file paths to MD5 value.
    ##
    def get_file_md5s(workdir)
      parts = {}

      # Get the Base64 encoded MD5 of each file
      Dir.entries(workdir).each do |file|
        next if file =~ /\.\./
        next if file =~ /\./

        full_path = "#{workdir}#{file}"

        md5 = Base64.encode64(Digest::MD5.file("#{full_path}").digest).chomp!

        parts[full_path] = md5
      end

      return parts
    end

    ##
    # Uploads a part of a file. 
    ##
    def upload_file_part(file_path, file_md5, part_number, object_key, upload_id, tags)

      # Reload to stop the connection timing out, useful when uploading large chunks
      aws.reload

      # Could use paralllel gem or similar here so that we get parallel 
      # upload when running on a ruby implementation that allows multithreading.
      puts "Starting on File: #{file_path} with MD5: #{file_md5} - this is part #{part_number}"

      part_upload = nil
      # Pass fog a file object to upload
      File.open(file_path) do |file_part|

        puts "Uploading #{file_part.path} [#{part_number}] to #{@bucket_name} as #{object_key} with upload id #{upload_id}"

        part_upload = aws.upload_part(@bucket_name, object_key, upload_id, part_number, file_part, { 'Content-MD5' => file_md5 } )

        # You need to make sure the tags array has the tags in the correct order, else the upload won't complete
        index = part_number-1
        tags[index] = part_upload.headers["ETag"]

        puts "Response: #{part_upload.inspect}" # This will return when the part has uploaded
      end

      return part_upload
    end

    ##
    # Removes the workdir and all files in it. 
    ##
    def clean_up_files(workdir)
      # clean up the tmp files
      puts "Cleaning up file split parts"
      FileUtils.remove_dir workdir
    end

    ##
    # Uploads a file in multiple parts. 
    #
    # The disk needs to have the a minimum of the file size free.
    # Expects object_to_upload to NOT have a leading /
    ##
    def upload_file_as_multipart(object_to_upload, object_key)

      # method based on gist from https://gist.github.com/908875
      # also https://gist.github.com/833374

      puts "Uploading file as multipart: file=#{object_to_upload}"

      # tmp dir to place the split file into
      cur_date = DateTime.now.strftime('%F-%T')
      workdir = "#{@tmpdir}/s3snapshot-splits/#{cur_date}/#{File.basename(object_to_upload)}/"
      FileUtils.mkdir_p(workdir)

      # Assumes we are running on unix with the split command available.
      # Split the file into chunks, max size of 1G, the chunks are 000, 001, etc
      # Smaller chunks increase the likelyhood of upload success.
      split_cmd = "split -b 1G -a 3 --verbose -d #{object_to_upload} #{workdir}"
      puts "split command: #{split_cmd}"
      split_result = system split_cmd

      if split_result
        puts "Split file into parts in #{workdir}"
      else
        puts "Split FAILED! exit code = #{$?}"
        # dont clean up, as user may need to see files to work out how to fix the split error.
        exit 1 # exit with non 0 error code as split failed.
      end  

      # Map of the file_part => md5
      parts = get_file_md5s workdir

      puts "File #{object_to_upload} has been split into #{parts.size} parts."
      ### Now ready to perform the actual upload

      # Initiate the upload and get the upload id
      # this keeps failing, may have to retry.

      multi_part_up = nil

      # retry up to 5 times before failing
      (1..MAX_RETRY_COUNT).each do |retry_count|
        begin 
          aws.reload
          multi_part_up = aws.initiate_multipart_upload(@bucket_name, object_key, { 'x-amz-acl' => 'private' } )
          # initiation successful, so break.
          break
        rescue Exception => e

          puts "multipart upload initiation FAILED: #{e.message}"

          if retry_count <= MAX_RETRY_COUNT
            puts "Retrying multipart upload initiation for the #{retry_count} time."
          else
            puts e.backtrace.join("\n")
            # fail this, we cant initiated the upload
            puts "Failed to initiate multipart upload after #{MAX_RETRY_COUNT} retries."
            exit 2
          end
        end
      end
      
      upload_id = multi_part_up.body["UploadId"]

      tags = []

      # sort based on the sufix provided by the split command., eg 001
      sorted_parts = parts.sort_by do |d|
        d[0].split('/').last.to_i
      end

      sorted_parts.each_with_index do |entry, idx|
        # Part numbers need to start at 1
        part_number = idx + 1

        # retry up to 5 times before failing
        (1..MAX_RETRY_COUNT).each do |retry_count|
          begin
            # file_path, file_md5, part_number, object_key, upload_id, idx, tags
            upload_file_part(entry[0], entry[1], part_number, object_key, upload_id, tags)
            # upload of part successful, so break.
            break
          rescue Exception => e
            puts "UPLOAD FILE PART FAILED: #{e.message}"
            
            if retry_count <= MAX_RETRY_COUNT
              puts "Retrying file part upload for #{entry[0]}"
            else
              puts e.backtrace.join("\n")
              # failed to upload file part
              puts "Failed to upload file part #{entry[0]} after #{MAX_RETRY_COUNT} retries."
              exit 3
            end
          end
        end

      end

      # retry up to 5 times before failing
      (1..MAX_RETRY_COUNT).each do |retry_count|
        begin
          completed_upload = aws.complete_multipart_upload(@bucket_name, object_key, upload_id, tags)
          puts "Completed Upload: #{completed_upload.inspect}"
          # multipart completed, so break.
          break
        rescue Exception => e
          puts "UPLOAD COMPLETED REQUEST FAILED: #{e.message}"
          
          if retry_count <= MAX_RETRY_COUNT
            puts "Retrying multipart upload complete"
          else
            puts e.backtrace.join("\n")
            # failed to comple multipart upload
            puts "Failed to complete multipart upload after #{MAX_RETRY_COUNT} retries."
            exit 4
          end
        end
      end

      clean_up_files workdir

    end
  end
end