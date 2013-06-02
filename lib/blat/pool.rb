
require 'thread'
require 'curl'

require 'blat/formats'

module Blat 

  # The Blat::Pool class controls a number of workers as they go about running
  # curl Jobs.  This is the main class of Blat, and is the most flexible way of
  # using the gem (Batch is simpler but less full-featured).
  #
  # == Workflow
  #
  # The pool is created with a size and a callback to present results to.
  # This callback may be presented as a proc object or as a block, and is
  # called with a finalised Blat::Job object upon completion of each request.
  #
  #  x = Blat::Pool.new(100){ |job|
  #         puts "#{job.data[:body]}"
  #      }
  #
  # Once a pool is configured, it may be commanded to start downloading by
  # presenting it with a dispatcher.  This is a procedure that returns either a
  # Blat::Job object or nil---workers will call this block in order to acquire
  # work, and will enter an idle state when nil is returned.
  #
  #  job_list = File.read('urls').lines.map{ |l| Blat::SimpleJob.new(l) }
  #  
  #  x.work{
  #   job_list.pop
  #  }
  #
  # Downloading can be waited upon any number of ways.  The status of the pool
  # may be requested with #count_idle and #all_idle? , and it's possible to
  # wait until idle using #wait_until_idle :
  #
  #  x.wait_until_idle
  #  x.close
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
  class Pool

    # Construct a new pool with a given size and a callback used to output
    # data.
    #
    #  x = Blat::Pool.new(100){ |job|
    #         puts "Job complete: #{job}"
    #      }
    #
    def initialize(size, finalise_callback = nil, &block)

      @m              = Mutex.new # Data mutex for "producer" status
      @t              = {} # threads
      @w              = [] # workers
      @idle           = []
      @idle_mutex     = Mutex.new
      @size           = size.to_i # number of simultaneous workers

      # Pass a block for handling returns
      if block
        @finalise_callback = block
      elsif finalise_callback && finalise_callback.is_a?(Proc)
        @finalise_callback = finalise_callback
      else
        raise 'No callback given for final data'
      end

    end

    # ------------------------------------------------------------------------
    # Workers call these to report status
    #

    # Workers can register as active by calling this
    def worker_active(worker_id)
      @idle_mutex.synchronize{ @idle[worker_id] = false }
    end

    # Workers can register as idle by calling this
    def worker_idle(worker_id)
      @idle_mutex.synchronize{ @idle[worker_id] = true }
    end

    # Workers can register that they have completed
    # a job by calling this.
    def work_complete(job)
      @finalise_callback.call(job) 
    end

    # ------------------------------------------------------------------------
    # Worker status
    #

    # check to see if all workers are idle
    def all_idle?
      @idle_mutex.synchronize{ @idle.inject(true) { |m, o| m && o} }
    end

    # Return the number of idle workers
    def count_idle
      @idle_mutex.synchronize{ @idle.count(true) }
    end

    # ------------------------------------------------------------------------
    # Set work and initialise workers
    #
    
    # Create workers without running them.
    #
    # This is usually not very useful to call on its own, and is called by
    # #work when creating threads.
    def init_workers
      #$log.debug "Maintaining #{@size} worker object[s] (#{@w.length} currently active)."
      @w = []
      (@size - @w.length).times do |s|
        @w << Worker.new(s, self)
        @idle[s] = true
      end
      #$log.info "#{@w.length} worker[s] created."
    end

    # Run a worker over every point competitively.
    # Will create @size workers if they do not already exist (there is no need
    # to also call init_workers)
    def work(dispatcher = nil, &block)

      raise "No dispatcher provided" unless block_given? || (dispatcher && dispatcher.is_?(Proc))

      init_workers

      # Make things do the work
      #$log.debug "Starting threads..."
      @start_time = Time.now
      @w.each do |w|
        # Give each worker a handle back to the dispatcher to get data.
        @t[w] = Thread.new(dispatcher || block) do |d|
          begin
            w.work(d)
          rescue SignalException => e
            #$log.fatal "Signal caught: #{e.message}"
            #$log.fatal "Since I'm sampling right now, I will kill workers before shutdown."
            kill_workers
            raise e
          end
        end

        # Pass exceptions up
        @t[w].abort_on_exception = true
      end
      #$log.info "#{@t.length} download thread[s] started."
    end

    # ------------------------------------------------------------------------
    # Wait on conditions and close the pool
    #

    # Block until all workers are idle, checking every poll_rate seconds.
    def wait_until_idle(poll_rate = 0.5)
      #$log.debug "Waiting until idle, polling every #{poll_rate}s..."
      sleep(poll_rate)
      sleep(poll_rate) until all_idle?
    end

    # Wait for threads to complete.
    def wait_until_closed
      #$log.debug "Waiting for #{@t.length} worker[s] to close."
      @t.each { |w, t| t.join }
      #$log.info "Workers all terminated naturally."
    end

    # Tell workers to die forcibly
    def kill_workers
      #$log.debug "Forcing #{@t.length} worker threads to die..."
      @t.each { |t| t.kill }
      #$log.info "Worker threads killed."
    end

    # Close all workers' connections to the servers cleanly,
    #
    # This is non-blocking.  Call #close or #wait to block:
    #
    #  pool.close_nonblock
    #  pool.wait_until_closed
    #
    def close_nonblock
      #$log.debug "Requesting closure of #{@w.length} worker[s]..."
      @w.each { |w| w.close }
    end

    # Cleanly close the pool, waiting for workers to end their
    # current request.  Blocks, unlike #close.
    def close
      close_nonblock
      wait_until_closed
    end

  private

    # Workers are instantiated and maintained by a Blat::Pool and continually
    # poll for available work, passing it off for integration with the final
    # results set.
    #
    # Though it is possible to create your own, I would recommend instead using
    # a pool.
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
    class Worker

      # Construct a new worker with a given ID and linked to a given pool.
      #
      # The pool will be called to report idle/working states.
      def initialize(id, pool)
        @id       = id
        @pool     = pool
        @abort    = false
      end

      # Should be run in a thread.  Performs work until the dispatcher runs 
      # out of data.
      def work(dispatcher)
        # start idle
        last_idle_state = true

        loop do
          while (job = dispatcher.call).is_a?(Job) do

            # If we were idle last, tell the pool
            @pool.worker_active(@id) if last_idle_state == true

            # tell people
            #$log.debug "W#{@id}: Downloading job #{job}"

            # Make the request
            complete_request(job, new_curl(job), job.config)

            return if @abort 
          end
          return if @abort

          # TODO: configurable
          @pool.worker_idle(@id)
          last_idle_state = true
          sleep(1)
        end

      # rescue StandardError => e
        #$log.warn "W#{@id}: Error: #{e}"
        #$log.debug "#{e.backtrace.join("\n")}"
      end

      # Closes the connection to the server
      def close
        @abort = true
      end

    private

      # Datapoint is complete, run callback
      def finalise(job, head, body, response_properties, response, error)
        job.finalise!( 
                      head: head,
                      body: body,
                      response_properties: response_properties,
                      response: response,
                      error: error
                     )

        @pool.work_complete(job)
      end

      # ---------- called by workers below this line

      # Submit a complete dp to the pool
      def complete_request(job, res, config)

        # Somewhere to store the body in a size-aware way
        body    = ''

        # If limiting body size, use a callback to handle incoming data
        if config[:max_body_size]
          ignore  = false

          res.on_body do |str|
            # Read up to the limit of bytes
            if !ignore && config[:max_body_size] && (body.length + str.length) > config[:max_body_size]
              body += str[0..(body.length + str.length) - config[:max_body_size]]
              #$log.warn "W#{@id}: Job #{job} exceeded byte limit (#{config[:max_body_size]}b)"
              ignore = true
            elsif not ignore
              body += str
            else
              # ignore data
            end

            # Have to return number of bytes to curb
            str.length
          end
        end

        # Perform a request prepared elsewhere,
        # can run alongside other requests
        res.perform

        # Load body directly from response if not using the system above
        body = res.body_str unless config[:max_body_size]

        # Load stuff out of response object.
        response_properties = {
          round_trip_time:    res.total_time,
          redirect_time:      res.redirect_time,
          dns_lookup_time:    res.name_lookup_time,
          effective_uri:      res.last_effective_url,
          code:               res.response_code,
          download_speed:     res.download_speed,
          downloaded_bytes:   res.downloaded_bytes || 0,
          truncated:          ignore == true
        }

        # write to datapoint list
        finalise(job, res.header_str, body, response_properties, res, nil)

      rescue SignalException => e
        raise e
      rescue StandardError => e
        # if e.class.to_s =~ /^Curl::Err::/ then
        #   #$log.debug "W#{@id}: Job #{job}: #{e.to_s[11..-1]}"
        # else
        #   #$log.error "W#{@id}: Exception retrieving #{job}: #{e.to_s}."
        #   #$log.debug "#{e.backtrace.join("\n")}"
        # end

        # write to datapoint list
        finalise(job, res.header_str, body, response_properties, res, e)
      end

      # Returns a new curl object to use downloading things.
      def new_curl(job)
        # Set up curl
        c = Curl::Easy.new

        # Configure the curl object
        job.configure(c)

        # Return it for work
        return c
      end

    end

  end

end

