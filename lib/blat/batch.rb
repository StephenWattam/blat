

require 'blat/pool'

module Blat


  class Batch
    def initialize(urls=[], config = {}, &block)

      # Config for each object
      @config       = config
      @curl_config  = block if block_given?

      # URLS in as a string
      @urls         = urls
      @urls_mx      = Mutex.new

      # Stores results as Job objects
      @results      = []
      @results_mx   = Mutex.new

      # Keep this to see if we have finished
      @url_count    = urls.length
    end

    # Run a batch with a given level of parallelism
    def run(workers)
      me = self
      workers = [workers, @urls.length].min

      x = Blat::Pool.new(workers){ |job|
        @results_mx.synchronize { @results << job }
      }

      # Set work to do
      x.work do
  
        url = @urls_mx.synchronize { @urls.pop }

        if(url)
          Blat::Job.new(@config){ |c|
           
            # Configure with block if appropriate
            @curl_config.yield(c) if @curl_config

            c.url= url
          }
        else
          nil
        end
      end

      x.wait_until_idle
      x.close_blocking
    end

    # Is the batch complete?
    def complete?
      @results_mx.synchronize do
        @results.length == @url_count
      end
    end

    # Report progress with three vars
    #
    # remaining (yet to do)
    # complete (completed)
    # in_progress (currently running)
    # total (remaining + complete + in progress)
    def progress
      remaining = @urls_mx.synchronize { @urls.length }
      complete  = @results_mx.synchronize { @results.length }
      return remaining, complete, (@url_count - complete - remaining), @url_count
    end

    # Return results.
    def results
      @results_mx.synchronize do
        @results
      end
    end

    private

    # Add a result to the internal lot
    def add_result(job)
    end

  end

end
