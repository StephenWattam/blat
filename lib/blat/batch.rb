

require 'curl'

module Blat

  module Blat::Batch

    # Blat::Batch::run takes a list of links and downloads them all before
    # returning.  It is a very simple interface to Curl::Multi for smallish
    # tasks.
    #
    # [max_connections] Defines how many parallel connections to use
    # [links] Is the list of strings or Curl::Easy objects to download.  The list object must support #map and #each
    # [pipeline] Indicates if Curl::Multi should pipeline its HTTP requests
    # [&block] If given, this block is called to configure each Curl::Easy object prior to it being pushed into the queue.
    #
    def self.run(max_connections, links, pipeline = true, &block)
      multi = Curl::Multi.new

      # Set options
      multi.max_connects  = max_connections.to_i
      multi.pipeline      = (pipeline == true)

      curls = links.map do |l|
        c = l
        c = Curl::Easy.new(l) unless l.is_a?(Curl::Easy)
        c
      end

      # Pump links in
      curls.each do |c|
        yield(c) if block_given?
        multi.add(c)
      end

      # Wait
      multi.perform

      return curls
    end
  end

end
