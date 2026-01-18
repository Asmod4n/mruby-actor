class Future
  def initialize(actor_obj, seq_num)
    @actor_obj = actor_obj
    @seq_num = seq_num
  end

  def unrwap
    @actor_obj.future_unwrap(@seq_num)
  end

  def ready?
    @actor_obj.future_ready?(@seq_num)
  end

  def wait_for(miliseconds_timeout)
    @actor_obj.future_wait_for(@seq_num, miliseconds_timeout)
  end
end
