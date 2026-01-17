module Actorizable
  @actor_classes = []

  # Expose the list of actor classes
  def self.actor_classes
    @actor_classes
  end

  # This runs whenever a class includes the module
  def self.included(base)
    @actor_classes << base
  end
end

class Testme
  include Actorizable
end

module Outer
  class Testme
    include Actorizable
  end
end

module Testcode
  class << self
    def run
      actor = InprocActor.new
      obj = actor.new(String, "Hello!")
      1000.times do
        obj.send_and_forget(:to_s)
      end
    end
  end
end
