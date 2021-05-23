This is currenty broken, because i had to remove threading from my mruby-zmq gem, if someone can fix that code a PR is very appreciated.

# mruby-actor
Breaking changes
================
The wire protocol is incompatible with earlier versions, its all msgpack now and is as such compatible to any programming language which has msgpack and zeromq support.
Discovery lends its idea from zyre, but uses a different wire protocol and only runs over multicast.
Each actor now runs in its own zmq context, this was needed so a clean shutdown is possible. The old mruby-actor had to be forced killed to close itself.

Preliminary
===========
mruby-actor is a library to bring distributed concurrency to mruby with the [actor model](https://en.wikipedia.org/wiki/Actor_model).
If you just need local threading based on the actor model for mruby take a look at [mruby-zmq](https://github.com/Asmod4n/mruby-zmq)


Blocking operations
===================
Blocking operations must be avoided at all costs. If you really need to block a mrb context do so in a ZMQ::Thread.
If you block a mruby-actor for longer than 15 seconds its considered to have crashed by other actors.

Security
========
All communication between mruby actors is authenticated and encrypted, except the Service Discovery, its going to be fully authenticated in another released.

Each Actor expects a Auth class and optionally Arguments for it, the args must be a Array. Take a look at https://github.com/Asmod4n/mruby-zmq/blob/a56c5722902a079777c28de67ad2aca8f499095a/mrblib/zap.rb#L54 how to implement it for your needs.
The mechanism used in mruby-actor is curve.
The authentication protocol used is https://rfc.zeromq.org/spec:27/ZAP/

Blocks, Procs, lambdas
----------------------
mruby-actor makes use of mrubys possibility to serialize and deserialize blocks of ruby code.
Those blocks work a bit differently than you are used to: they have no way to capture the environment around them.
How it works: if you pass a code block it gets serialized, send over the network to the remote actor, gets deserialized and executed there.
Yes, that is literary remote code execution; to build a safeguard around it mruby-actor got completely rewritten to authenticate every peer which wants to connect.
Also, this gem won't be turned into a mgem until mruby 1.3 is released, there are still some issues which have to be addressed: https://github.com/mruby/mruby/issues/created_by/clayton-shopify

Examples
========
```ruby
keypair = LibZMQ.curve_keypair
keypair2 = LibZMQ.curve_keypair
actor = Actor.new(auth: {class: ZMQ::Zap::Authenticator}, server_endpoint: "tcp://en0:*", keypair: keypair)
actor2 = Actor.new(auth: {class: ZMQ::Zap::Authenticator}, keypair: keypair2)
actor3 = Actor.new(auth: {class: ZMQ::Zap::Authenticator, args: ["hallo", "actor"]})
actor4 = Actor.new(auth: {class: ZMQ::Zap::Authenticator})
# if you are on Windows, the mandatory options besides tha auth hash is :server_endpoint and :broadcast_address, the server endpoint must be something that works locally and from other hosts it wants to interact with, the broadcast address must be the broadcast address of the server endpoint. On other platforms it automatically picks the first running network interface with a broadcast address it can find and binds to a random port.
# Take a look at http://api.zeromq.org/master:zmq-bind which endpoints are available in zmq, wildcard tcp ports are supported.
# the inproc transport isn't supported by design, each actor runs in a seperate zmq context, inproc sockets can only communicate in their own zmq context.
sleep 1 # requires conf.gem mgem: 'mruby-sleep' in your build_config.rb
string = actor.remote_new(actor.peers.first.first, String, "hallo")
string.async(:upcase!)
string.send(:to_str)
string.to_str
string.async(:each_byte) {|byte|puts byte.chr} # create a actor in another terminal to see this codes gets executed on the remote actor
```

License
=======
   Copyright 2015,2017 Hendrik Beskow

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this project except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
