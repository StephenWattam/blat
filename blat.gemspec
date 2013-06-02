
Gem::Specification.new do |s|
  # About the gem
  s.name        = 'blat'
  s.version     = '0.1.0a'
  s.date        = '2013-06-02'
  s.summary     = 'Aggressive parallel web request library'
  s.description = 'A very parallel cURL wrapper for ongoing download tasks'
  s.author      = 'Stephen Wattam'
  s.email       = 'stephenwattam@gmail.com'
  s.homepage    = 'http://stephenwattam.com/projects/blat'
  s.required_ruby_version =  ::Gem::Requirement.new(">= 1.9")
  s.license     = 'Beerware'
  
  # Files + Resources
  s.files         = Dir.glob("lib/blat/*.rb") + ['./lib/blat.rb']
  s.require_paths = ['lib']
  
  # Documentation
  s.has_rdoc         = true 

  # Deps
  s.add_runtime_dependency 'curb',       '~> 0.8'

  # Misc
  s.post_install_message = "Thanks for installing Blat!"
end


