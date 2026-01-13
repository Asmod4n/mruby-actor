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

module Testcode
  class << self
    def run
      actor = Actor.new
      obj = actor.new_inproc(String, "15")
      1000000.times do
        obj.call_and_forget(:to_i, 10)
      end
    end
  end
end
