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
