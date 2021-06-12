MRuby::Gem::Specification.new('mruby-actor') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'actor library for mruby'
  spec.add_dependency 'mruby-zmq'
  spec.add_dependency 'mruby-method'
  spec.add_dependency 'mruby-chrono'
  spec.add_dependency 'mruby-uri-parser'
  spec.add_dependency 'mruby-ipaddr'
  spec.add_dependency 'mruby-sleep'
end
