class Actor
  class Error < StandardError; end
  class ProtocolError < Error; end

  class Proxy
    def initialize(actor, object_id)
      @actor = actor
      @object_id = object_id
    end

    def send(m, *args)
      @actor.send(@object_id, m, *args)
    end

    alias :<< :send
  end
end
