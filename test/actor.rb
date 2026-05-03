assert('Actor: build a String inside an actor and call methods on it') do
  a = Actor.new
  s = a.create(String, "hello")
  assert_equal 5, s.call(:length)
  assert_equal "HELLO", s.call(:upcase)
end

assert('Actor: fire-and-forget mutates state seen by later calls') do
  a   = Actor.new
  arr = a.create(Array)
  arr.call_and_forget(:push, 1)
  arr.call_and_forget(:push, 2)
  arr.call_and_forget(:push, 3)
  assert_equal [1, 2, 3], arr.call(:to_a)
  assert_equal 3, arr.call(:length)
end

assert('Actor: each actor has an independent VM') do
  a1 = Actor.new
  a2 = Actor.new
  s1 = a1.create(String, "first")
  s2 = a2.create(String, "second")
  assert_equal "FIRST",  s1.call(:upcase)
  assert_equal "SECOND", s2.call(:upcase)
end

assert('Actor: exception raised in actor is re-raised in caller') do
  a = Actor.new
  s = a.create(String, "x")
  err = assert_raise(NoMethodError) { s.call(:nonexistent_method) }
  assert_include err.message, 'nonexistent_method'
end

assert('Actor: arguments and return values round-trip via CBOR') do
  a = Actor.new
  h = a.create(Hash)
  h.call_and_forget(:store, "k", [1, 2, 3])
  h.call_and_forget(:store, "n", 42)
  assert_equal [1, 2, 3], h.call(:[], "k")
  assert_equal 42,        h.call(:[], "n")
end

assert('Actor: object_id is exposed') do
  a = Actor.new
  s = a.create(String, "x")
  assert_kind_of Integer, s.object_id
end
