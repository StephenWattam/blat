

module Blat
  
 

  class Job
    
    attr_reader :config, :data 

    # Construct with a config for a curl object
    def initialize(config={}, &block)
      raise "No curl configuration block given" if not block_given?

      @curl_config_block = block
      @config = config
      @finalised = false
    end

    # Configure a curl object to make the request
    def configure(curl)
      @curl_config_block.yield(curl)
    end

    # Has this job been completed?
    def finalised?
      @finalise
    end

    alias :closed? :finalised?

    # Write result and prevent further editing
    def finalise!(data = {})
      raise "Job is already finalised." if finalised?
      @data = data
      @finalised = true
    end
  end

  def SimpleJob
    def initialize(url, curl_config = {}, config = {})
      curl_config.merge!({url: url})

      super(config){ |c|
        curl_config.each do |k,v|
          if v.is_a?(Array)
            curl.send(k.to_s + '=', *v) 
          else
            curl.send(k.to_s + '=', v) 
          end
        end
      }
    end
  end


end
