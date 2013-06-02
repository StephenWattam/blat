
require 'blat/pool'
require 'blat/batch'
require 'blat/formats'

# Blat is a hugely parallel wrapper for Curl designed to download data as aggressively as possible.
#
# Blat makes use of many threads at once in a producer-consumer pattern, and accepts tasks in the form 
# of Blat::Jobs, which contain configuration and results from each request.
module Blat

  VERSION = '0.0.1a'

end
