

module Blat

  # Blat::Job represents a single download task, both as a request and response.
  #
  # Jobs are provided to workers in a pool by a dispatcher block.  Each job
  # contains:
  #
  # * Configuration for the worker.  Current configuration supported is
  # detailed below and in the Pool documentation
  # * A way of configuring a curl request (in order to set the url and other
  # parameters)
  # * Data returned by the download.  This is stored as a hash in the #data
  # parameter.
  #
  # == Worker Configuration
  #
  # Workers are configured by setting values in a hash.  This hash is sent to
  # the worker from the Job class, and contains options that affect the process
  # of downloading.  This is in addition to configuration on the curl object
  # performed through Blat::Job.configure()
  #
  # Workers currently support the following configuration options:
  #
  # [:max_body_size] If set, downloads will cease after this many bytes have
  # been downloaded.  If truncated, data[:response_properties][:truncated] will
  # be set to true.
  #
  # == Returned Values
  #
  # When a job has been finalised, its #data property will be set to a hash
  # left by the worker.  This is currently specified as:
  #
  # [:head] The head string returned from the server (response.header_str)
  # [:body] The body string returned from the server (response.body)
  # [:response_properties] A hash with metadata in.  Partially specified by the
  # worker configuration, this contains things such as the number of bytes
  # downloaded and duration of the request.
  # [:response] The raw response from curl
  # [:error] Any errors encountered during download, such as network errors.
  # If this is nil the request was successful.
  # 
  # Response properties are currently set to:
  #
  #  response_properties = {
  #    round_trip_time:    res.total_time,
  #    redirect_time:      res.redirect_time,
  #    dns_lookup_time:    res.name_lookup_time,
  #    effective_uri:      res.last_effective_url,
  #    code:               res.response_code,
  #    download_speed:     res.download_speed,
  #    downloaded_bytes:   res.downloaded_bytes || 0,
  #    truncated:          ignore == true
  #  }
  #
  class Job

    attr_reader :config, :data 

    # Construct a new Job with a block for configuring curl options.
    def initialize(config = {}, &block)
      raise 'No curl configuration block given' unless block_given?

      @curl_config_block  = block
      @config             = config
      @finalised          = false
    end

    # Configure a curl object to make the request
    def configure(curl)
      @curl_config_block.yield(curl)
    end

    # Has this job been completed?
    def finalised?
      @finalise
    end

    # Allow people to use closed? instead.
    alias :closed? :finalised?

    # Write result and prevent further editing
    def finalise!(data = {})
      raise 'Job is already finalised.' if finalised?
      @data = data
      @finalised = true
    end
  end

  # --------------------------------------------------------------------------
  

  # SimpleJob is a quick and easy way of wrapping a URL to create a job.
  #
  # It accepts:
  #
  # [:url] The URL to download
  # [:curl_config] A hash of properties to set on the curl object, for example: {'follow_location' => true}
  # [:config] The worker configuration properties.
  class SimpleJob < Job
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
