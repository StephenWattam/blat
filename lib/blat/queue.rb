
require 'curl'

module Blat

  # The Blat::Queue class represents a download queue that handles requests
  # using Curl::Multi.  It, and its descendants, accept a large number of
  # Curl::Easy objects and download them in parallel.
  #
  # In order to know when each request has completed, use
  # Curl::Easy::on_complete.  This is made simpler by Queue#add, which will
  # yield to a block on completion of each download.
  #
  class Queue

    attr_reader :max_connections, :pipeline

    # Create a new Blat::Queue with a given number of maximum connections.
    #
    # The 'pipeline' options controls Curl::Multi's pipelining feature, which
    # tries to use the same http connection for many requests to the same server.
    def initialize(max_connections, pipeline = true)
      @multi = Curl::Multi.new

      # Set properties
      @max_connects         = max_connections.to_i
      @pipeline             = (pipeline == true)
      @multi.max_connects   = @max_connects
      @multi.pipeline       = @pipeline
    end

    # Add a URL or a Curl::Easy object to the queue.
    #
    # Optionally, provide a callback for calling when requests are complete,
    # e.g.:
    #
    #  q.add('http://google.com') do |c|
    #    puts "Complete request: #{r}"
    #  end
    #
    def add(curl_or_link, &block)
      # Convert to curl if necessary
      curl = curl_or_link.is_a?(Curl::Easy) ? curl_or_link : Curl::Easy.new(curl_or_link)
      curl.on_complete { |c| block.yield(c) } if block_given?

      # Add
      @multi.add(curl)

      # Return
      return curl
    end

    # Returns the number of active requests
    def request_count
      requests.length
    end

    # Returns a list of active requests
    def requests
      @multi.requests
    end

    # Remove a request from the queue.
    #
    # This needn't be called if a request has completed.
    def remove(curl)
      @multi.remove(curl)
    end

    # Wait for all requests to finish (blocking).
    #
    # If a block is given it is executed repeatedly whilst waiting.
    def wait(&block)
      @multi.perform do
        yield if block_given?
      end
    end

    alias_method :perform, :wait

    # Is the queue idle?
    def idle?
      @multi.idle?
    end

  end

  # Similar to a queue, except that it explicitly calls a block in order to
  # acquire new URLs.  
  #
  # This makes it suitable for use in producer/consumer patterns.
  class ConsumingQueue < Queue 

    # Executes the given block in order to keep the curl pool working at its
    # maximum capacity.
    #
    # consume blocks as long as links are being downloaded, as it relies on
    # Curl::Multi#perform
    #
    # Note that blocks providing links must also perform their own
    # configuration, e.g.:
    #
    #  q.consume do
    #    url = get_url
    #    if(url)
    #      c = Curl::Easy.new(url)
    #      c.follow_location = true
    #      c.on_complete{ |c| puts "Retrieved: #{c.body_str}" }
    #      c
    #    else
    #      nil
    #    end
    #  end
    #
    def consume(connections = @max_connects, &block)
      @multi.perform do
        while request_count < connections && new_link = yield
          add(new_link) if new_link
        end
      end
    end

  end

  # The ListConsumingQueue is similar to the ConsumingQueue except that
  # it takes its argument in the form of an Enumerable object.
  class ListConsumingQueue < ConsumingQueue

    # Download all of the URLs or Curl::Easy objects in the given list, and
    # optionally execute the given block on completion for each
    def consume(list, connections = @max_connects)
      item = 0            # Start at item 0
      list = list.to_a    # Ensure we can address with []

      @multi.perform do
        while request_count < connections && new_link = list[item]

          item += 1

          # Add with config block if appropriate
          if block_given?
            add(new_link) { |req| yield(req) }
          else
            add(new_link)
          end

        end
      end
    end
  end

end
