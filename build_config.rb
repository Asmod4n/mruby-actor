MRuby::Build.new do |conf|
  toolchain :gcc
  enable_debug
  #conf.enable_sanitizer "address,undefined"
  #conf.cxx.flags << '-fno-omit-frame-pointer'
  #conf.linker.flags_before_libraries << '-static-libasan'
  conf.cxx.flags << '-O3' << '-march=native' << '-std=c++23'
  conf.cc.flags << '-O3' << '-march=native'
  conf.enable_debug
  conf.enable_test
  conf.gembox 'default'
  conf.gem File.expand_path('../../mruby-zmq')
  conf.gem File.expand_path(File.dirname(__FILE__))
end
