
require 'thread'
require 'curl'

require 'blat/formats'

module Blat 











  # -----------------------------------------------------------------------------------------------------
  class Pool 
    def initialize(size, finalise_callback=nil, &block)

      @m    = Mutex.new # Data mutex for "producer" status
      @t    = {} #threads
      @w    = [] # workers
      @idle = []
      @idle_mutex = Mutex.new
      @size = size.to_i # number of simultaneous workers

      # Pass a block for handling returns
      if block
        @finalise_callback = block
      elsif finalise_callback and finalise_callback.is_a?(Proc)
        @finalise_callback = finalise_callback
      else
        raise "No callback given for final data"
      end

    end

    # Workers can register as active by calling this
    def worker_active(worker_id)
      @idle_mutex.synchronize{
        @idle[worker_id] = false
      }
    end

    # Workers can register as idle by calling this
    def worker_idle(worker_id)
      @idle_mutex.synchronize{
        @idle[worker_id] = true
      }
    end

    # Workers can register that they have completed
    # a job by calling this.
    def work_complete(job)
      @finalise_callback.call( job ) 
    end

    # check to see if all workers are idle
    def all_idle?
      @idle_mutex.synchronize{
        @idle.inject(true){ |m, o| m and o}
      }
    end

    # Return the number of idle workers
    def count_idle
      @idle_mutex.synchronize{
        @idle.count(true)
      }
    end

    # Create workers without running them.
    #
    # This is usually not very useful to call on its own, and is called by
    # #work when creating threads.
    def init_workers
      #$log.debug "Maintaining #{@size} worker object[s] (#{@w.length} currently active)."
      @w = []
      (@size - @w.length).times{|s|
        @w << Worker.new(s, self)
        @idle[s] = true
      }
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
      @t.each{|t|
        t.kill
      }
      #$log.info "Worker threads killed."
    end

    # Close all workers' connections to the servers cleanly,
    #
    # This is non-blocking.  Call #close_blocking or #wait to block:
    #
    #  pool.close
    #  pool.wait_until_closed
    #
    def close
      #$log.debug "Requesting closure of #{@w.length} worker[s]..."
      @w.each{|w| w.close }
    end

    # Cleanly close the pool, waiting for workers to end their
    # current request.  Blocks, unlike #close.
    def close_blocking
      close
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

      # Should be run in a thread.  Performs work until the dispatcher runs out of data.
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

      rescue StandardError => e
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
        body = ""
        ignore = false

        res.on_body do |str|
          # Read up to the limit of bytes
          if not ignore and config[:max_body_size] and (body.length + str.length) > config[:max_body_size] then
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

        # Perform a request prepared elsewhere,
        # can run alongside other requests
        res.perform

        # Output the result to debug log
        #$log.debug "W#{@id}: Completed request #{job}, response code #{res.response_code}."

        # Fix encoding of head if required
        head                = res.header_str


        # Load stuff out of response object.
        response_properties = {round_trip_time:    res.total_time,
          redirect_time:      res.redirect_time,
          dns_lookup_time:    res.name_lookup_time,
          effective_uri:      res.last_effective_url,
          code:               res.response_code,
          download_speed:     res.download_speed,
          downloaded_bytes:   res.downloaded_bytes || 0,
          truncated:          ignore == true
        }


        # write to datapoint list
        finalise(job, head, body, response_properties, res, nil)

      rescue SignalException => e
        raise e
      rescue StandardError => e
        if e.class.to_s =~ /^Curl::Err::/ then
          #$log.debug "W#{@id}: Job #{job}: #{e.to_s[11..-1]}"
        else
          #$log.error "W#{@id}: Exception retrieving #{job}: #{e.to_s}."
          #$log.debug "#{e.backtrace.join("\n")}"
        end

        # write to datapoint list
        finalise(job, head, body, response_properties, res, e)
      end



      # Returns a new curl object to use downloading things.
      def new_curl(job)
        # Set up curl
        c = Curl::Easy.new

        job.configure(c)

        return c
      end

    end


  end


end

