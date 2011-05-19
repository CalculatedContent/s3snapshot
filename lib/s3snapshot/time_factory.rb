module S3snapshot
  ##
  #Simple time factory that will return the set time if set, otherwise it returns the current time in utc
  ##
  class TimeFactory
    @@set_time = nil
    
    def self.utc_time
      if @@set_time.nil?
        return Time.now.utc
      end
      
      return @@set_time
    
    end

    
    def self.set_time(time)
      @@set_time = time
    end
    
    def self.unset_time
      @@set_time = nil
    end
    
  end
end