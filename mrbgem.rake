require_relative 'mrblib/version'

MRuby::Gem::Specification.new('mruby-actor') do |spec|
  spec.license  = 'Apache-2'
  spec.summary  = 'A actor framework for distributed mruby'
  spec.homepage = 'https://github.com/Asmod4n/mruby-actor'
  spec.authors  = 'Hendrik Beskow'
  spec.version  = Actor::VERSION

  spec.add_dependency 'mruby-errno'
  spec.add_dependency 'mruby-error'
  spec.add_dependency 'mruby-c-ext-helpers'
  spec.add_dependency 'mruby-simplemsgpack'
  spec.add_dependency 'mruby-zmq', '>= 1.0.0'
end
