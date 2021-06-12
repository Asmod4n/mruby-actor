unless LibZMQ.has? "curve"
  fail NotImplementedError, "mruby-actor needs libzmq with curve support"
end

class Actor < ZMQ::Thread
  include ZMQ::ThreadConstants

  REMOTE_NEW = 4.freeze
  REMOTE_SEND = 5.freeze
  REMOTE_ASYNC = 6.freeze
  REMOTE_PEERS = 7.freeze

  PEERS = 3.freeze

  def self.new(options = {}, &block) # this starts the background thread
    super(Actor_fn, options, &block)
  end

  def remote_new(endpoint, mrb_class, *args, &block)
    LibZMQ.send(@pipe, [REMOTE_NEW, endpoint, mrb_class, args, block].to_msgpack, 0)
    msg = MessagePack.unpack(@pipe.recv.to_str)
    case msg[TYPE]
    when INSTANCE
      RemoteThreadProxy.new(self, endpoint, msg[1])
    when EXCEPTION
      raise msg[1]
    end
  end

  def remote_send(endpoint, object_id, method, *args, &block)
    LibZMQ.send(@pipe, [REMOTE_SEND, endpoint, object_id, method, args, block].to_msgpack, 0)
    msg = MessagePack.unpack(@pipe.recv.to_str)
    case msg[TYPE]
    when RESULT
      msg[1]
    when EXCEPTION
      raise msg[1]
    end
  end

  def remote_async(endpoint, object_id, method, *args, &block)
    LibZMQ.send(@pipe, [REMOTE_ASYNC, endpoint, object_id, method, args, block].to_msgpack, 0)
  end

  REMOTE_PEERS_MSG = [REMOTE_PEERS].to_msgpack.freeze

  def peers
    LibZMQ.send(@pipe, REMOTE_PEERS_MSG, 0)
    msg = MessagePack.unpack(@pipe.recv.to_str)
    case msg[TYPE]
    when PEERS
      msg[1]
    when EXCEPTION
      raise msg[1]
    end
  end

  class RemoteThreadProxy
    attr_reader :object_id

    def initialize(thread, endpoint, object_id)
      @thread = thread
      @endpoint = endpoint
      @object_id = object_id
    end

    def send(m, *args, &block)
      @thread.remote_send(@endpoint, @object_id, m, *args, &block)
    end

    def async(m, *args, &block)
      @thread.remote_async(@endpoint, @object_id, m, *args, &block)
      self
    end

    def respond_to?(m)
      super(m) || @thread.remote_send(@endpoint, @object_id, :respond_to?, m)
    end

    def method_missing(m, *args, &block)
      @thread.remote_send(@endpoint, @object_id, m, *args, &block)
    end
  end

  class Actor_fn < ZMQ::Thread::Thread_fn # this is the background thread
    include ZMQ::ThreadConstants

    VERSION = '3'
    MRE = "MRE\1".freeze
    N = 'n'.freeze

    def initialize(options = {}, &block)
      @options = options
      @interrupted = false
      @instances = {}
      @remote_clients = {}
      @peers = {}
      @poller = ZMQ::Poller.new
      @timers = ZMQ::Timers.new
      @group = sprintf('%s-%s', self.class.name, @options.fetch(:version, VERSION))
      @keypair = @options.fetch(:keypair) { LibZMQ.curve_keypair }
      @poller << @pipe

      @auth = ZMQ::Zap.new(authenticator: @options[:auth].fetch(:class).new(*@options[:auth].fetch(:args) { [] } ))
      @poller << @auth

      @server = ZMQ::Router.new
      @poller << @server
      @server.router_handover = true
      @server.curve_security(type: :server, secret_key: @keypair.fetch(:secret_key), zap_domain: @group)
      @server.bind(@options.fetch(:server_endpoint) { sprintf('tcp://%s:*', ZMQ.network_interfaces.first) })
      @server_endpoint = @server.last_endpoint
      server_uri = URI.parse(@server_endpoint)

      @multicast_msg = "#{MRE}#{@keypair[:public_key]}#{[server_uri.port].pack(N)}#{@group}"
      if @multicast_msg.bytesize > 256
        fail ArgumentError, "group too large for multicast_msg"
      end
      @multicast_msg.freeze
      @multicaster = UDPSocket.new(@server.ipv6? ? Socket::AF_INET6 : Socket::AF_INET)
      @poller << @multicaster
      @multicaster.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
      if Socket.const_defined? "SO_REUSEPORT"
        @multicaster.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEPORT, 1)
      end
      if @server.ipv6?
        @multicast_address = "ff02::1"
        membership = IPAddr.new(@multicast_address).hton + IPAddr.new(server_uri.host).hton
        #@multicaster.setsockopt(Socket::IPPROTO_IPV6, Socket::IPV6_MULTICAST_IF, Actor.if_nametoindex(server_endpoint.host))
        @multicaster.setsockopt(Socket::IPPROTO_IPV6, Socket::IP_ADD_MEMBERSHIP, membership)
        @multicaster.bind('::', 5670)
      else
        @multicast_address = "224.0.0.1"
        local_if = IPAddr.new(server_uri.host).hton
        @multicaster.setsockopt(Socket::IPPROTO_IP, Socket::IP_MULTICAST_IF, local_if)
        membership = IPAddr.new(@multicast_address).hton + local_if
        @multicaster.setsockopt(Socket::IPPROTO_IP, Socket::IP_ADD_MEMBERSHIP, membership)
        @multicaster.bind('0.0.0.0', 5670)
      end
      @multicaster.send(@multicast_msg, 0, @multicast_address, 5670)
      @multicast_timer = @timers.add(1000, &method(:multicast_timer))
      @expire_timer = @timers.add(5000, &method(:expire_timer))
    end

    def run
      until @interrupted
        @poller.wait(@timers.timeout) do |socket, events|
          case socket
          when @pipe
            handle_pipe
          when @auth
            @auth.handle_zap
          when @multicaster
            handle_discovery
          when @server
            handle_server
          end
        end
        @timers.execute
      end
    ensure
      @multicaster.send("#{MRE}#{@keypair[:public_key]}#{[0].pack(N)}#{@group}", 0, @multicast_address, 5670)
      usleep(16)
    end

    def multicast_timer(timer_id)
      @multicaster.send(@multicast_msg, 0, @multicast_address, 5670)
    rescue => e
      ZMQ.logger.crash(e)
    end

    def expire_timer(timer_id)
      now = Chrono::Steady.now
      @peers.select {|_, peer| now >= peer[:expired_at]}.each_key do |endpoint|
        @peers.delete(endpoint)
        @remote_clients.delete(endpoint)
      end
    end

    ENDPOINT_FORMAT = "tcp://%s:%i".freeze

    def handle_discovery
      msg, address = @multicaster.recvfrom(256)
      if msg.start_with?(MRE) && msg.bytesize == 46 + @group.bytesize && msg.byteslice(46..-1) == @group
        port = msg.byteslice(44..45).unpack(N).first
        endpoint = sprintf(ENDPOINT_FORMAT, address.last, port)
        if endpoint != @server_endpoint
          if port == 0
            @peers.delete(endpoint)
            @remote_clients.delete(endpoint)
          elsif @peers.include?(endpoint)
            @peers[endpoint][:expired_at] = Chrono::Steady.now + 15
          else
            @peers[endpoint] = {
              expired_at: Chrono::Steady.now + 15,
              public_key: msg.byteslice(4..43)
            }
          end
        end
      end
    rescue => e
      ZMQ.logger.crash(e)
    end

    def remote_client(endpoint)
      @remote_clients.fetch(endpoint) do
        client = ZMQ::Dealer.new
        client.curve_security(type: :client, server_key: @peers[endpoint][:public_key],
          public_key: @keypair[:public_key], secret_key: @keypair[:secret_key])
        client.rcvtimeo = 120000
        client.sndtimeo = 120000
        client.connect(endpoint)
        @remote_clients[endpoint] = client
        client
      end
    end

    def handle_pipe
      msg = @pipe.recv
      msg_str = msg.to_str
      if msg_str == TERM
        @interrupted = true
      else
        begin
          msg_val = MessagePack.unpack(msg_str)
          case msg_val[TYPE]
          when NEW
            instance = msg_val[1].new(*msg_val[2], &msg_val[3])
            id = instance.__id__
            LibZMQ.send(@pipe, [INSTANCE, id].to_msgpack, 0)
            @instances[id] = instance
          when SEND
            LibZMQ.send(@pipe, [RESULT, @instances[msg_val[1]].__send__(msg_val[2], *msg_val[3], &msg_val[4])].to_msgpack, 0)
          when ASYNC
            begin
              @instances[msg_val[1]].__send__(msg_val[2], *msg_val[3], &msg_val[4])
            rescue LibZMQ::ETERMError => e
              raise e
            rescue => e
              ZMQ.logger.crash(e)
            end
          when FINALIZE
            @instances.delete(msg_val[1])
          when REMOTE_NEW, REMOTE_SEND
            client = remote_client(msg_val[1])
            LibZMQ.msg_send(msg, client, 0)
            LibZMQ.msg_send(client.recv, @pipe, 0)
          when REMOTE_ASYNC
            LibZMQ.msg_send(msg, remote_client(msg_val[1]), 0)
          when REMOTE_PEERS
            LibZMQ.send(@pipe, [PEERS, @peers].to_msgpack, 0)
          end
        rescue => e
          LibZMQ.send(@pipe, [EXCEPTION, e].to_msgpack, 0)
        end
      end
    rescue => e
      ZMQ.logger.crash(e)
    end

    def handle_server
      peer, msg = @server.recv
      begin
        msg = MessagePack.unpack(msg.to_str)
        case msg.fetch(TYPE)
        when REMOTE_NEW
          instance = msg.fetch(2).new(*msg.fetch(3), &msg[4])
          id = instance.__id__
          instance_msg = [INSTANCE, id].to_msgpack
          LibZMQ.msg_send(peer, @server, LibZMQ::SNDMORE)
          LibZMQ.send(@server, instance_msg, 0)
          @instances[id] = instance
        when REMOTE_SEND
          result = [RESULT, @instances.fetch(msg.fetch(2)).__send__(msg.fetch(3), *msg.fetch(4), &msg[5])].to_msgpack
          LibZMQ.msg_send(peer, @server, LibZMQ::SNDMORE)
          LibZMQ.send(@server, result, 0)
        when REMOTE_ASYNC
          if (instance = @instances[msg[2]])
            begin
              instance.__send__(msg.fetch(3), *msg.fetch(4), &msg[5])
            rescue LibZMQ::ETERMError => e
              raise e
            rescue => e
              ZMQ.logger.crash(e)
            end
          end
        end
      rescue => e
        exec_msg = [EXCEPTION, e].to_msgpack
        LibZMQ.msg_send(peer, @server, LibZMQ::SNDMORE)
        LibZMQ.send(@server, exec_msg, 0)
      end
    rescue => e
      ZMQ.logger.crash(e)
    end
  end
end


class String
  unless method_defined? :byteslice
  #
  # Does the same thing as String#slice but
  # operates on bytes instead of characters.
  #
  CSTAR = 'C*'

    def byteslice(*args)
      unpack(CSTAR).slice(*args).pack(CSTAR)
    end
  end
end
