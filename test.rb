actor = Actor.new
obj = actor.new_inproc(String, "Hello, Actor!" )
100000.times do
  obj.call_and_forget(:to_s)
end
