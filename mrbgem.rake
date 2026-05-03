require_relative 'mrblib/version'

MRuby::Gem::Specification.new('mruby-actor') do |spec|
  spec.license  = 'Apache-2'
  spec.summary  = 'In-process actor framework for mruby'
  spec.homepage = 'https://github.com/Asmod4n/mruby-actor'
  spec.authors  = 'Hendrik Beskow'
  spec.version  = Actor::VERSION

  spec.add_dependency 'mruby-errno'
  spec.add_dependency 'mruby-error'
  spec.add_dependency 'mruby-c-ext-helpers'
  spec.add_dependency 'mruby-cbor', github: 'Asmod4n/mruby-cbor', branch: 'main'
  spec.add_dependency 'mruby-str-constantize', github: 'Asmod4n/mruby-str-constantize', branch: 'main'

  spec.cxx.flags << '-std=c++17' << '-pthread'
  spec.linker.flags << '-pthread'
end
