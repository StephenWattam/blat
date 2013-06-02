

require 'blat/pool'

module Blat

  # The batch downloader is a simpler wrapper around Pool that runs in a
  # blocking manner.  The idea of this is that you can put a list of URLs in,
  # run #run and then retrieve the results easily and quickly.
  #
  # example:
  #
  #   urls = File.read('url.list').lines
  #   b = Blat::Batch.new( list )
  #   b.run(10)
  #   puts "Results: #{b.results}"
  #
  class Batch

    # Create a new batch downloader for a given list of URLS, and a given set
    # of configuration options.
    #
    # [:urls] An array of URLs to download.
    # [:config] (optional) configuration to pass to the Jobs.  See Blat::Job
    # for more information.
    def initialize(urls, config = {})

      # Config for each object
      @config       = config

      # URLS in as a string
      @urls         = urls
      @urls_mx      = Mutex.new

      # Stores results as Job objects
      @results      = []
      @results_mx   = Mutex.new

      # Keep this to see if we have finished
      @url_count    = urls.length
    end

    # Run a batch with a given number of workers.
    #
    # If a block is provided, it is called with the curl object just before
    # requests are made.  This is to allow setting of various parameters, e.g.:
    #
    #  batch.run(10){ |c|
    #     c.follow_location = true
    #  }
    #
    def run(workers, &block)

      # Figure out if people have overestimated the workers needed
      workers = [workers, @urls.length].min

      # Construct a pool
      x = Blat::Pool.new(workers) do |job|
        @results_mx.synchronize { @results << job }
      end

      # Set work to do
      x.work do

        # Get the URL from the list
        url = @urls_mx.synchronize { @urls.pop }

        # If it's set, configure and return a job
        if url
          Blat::Job.new(@config) do |c|

            # Configure with block if appropriate
            yield(c) if block_given?

            c.url= url
          end
        else
          # If not, return nil to set the worker to idle
          nil
        end
      end

      # Wait until workers are idle
      x.wait_until_idle

      # Close them all.
      x.close
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
      return remaining,
             complete,
             (@url_count - complete - remaining),
             @url_count
    end

    # Get results as a list
    def results
      @results_mx.synchronize do
        return @results
      end
    end

  end

end
