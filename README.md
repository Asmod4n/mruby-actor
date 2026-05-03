# mruby-actor

A minimal in-process actor framework for mruby.

Each actor runs in its own OS thread with a fresh `mrb_open()` VM.
Cross-actor data is serialised with [mruby-cbor](https://github.com/Asmod4n/mruby-cbor).

This first cut intentionally has no remote / IO / authentication layer —
those will be added later. The wire codec (CBOR) and message envelope are
designed to make that straightforward.

## Example

```ruby
class Counter
  def initialize(start = 0)
    @n = start
  end

  def increment
    @n += 1
  end

  def value
    @n
  end
end

actor   = Actor.new                    # spawn an actor thread
counter = actor.create(Counter, 10)    # build a Counter inside the actor

counter.call(:increment)               # => 11  (synchronous, returns value)
counter.call_and_forget(:increment)    # => nil (fire-and-forget)
counter.call(:value)                   # => 12

# When `actor` and `counter` go out of scope, the thread is shut down
# cleanly and the underlying mrb_state is closed.
```

Any class that can be instantiated inside the actor's fresh `mrb_state`
can be used — typically that means classes defined in mrbgems compiled
into the build.

Method arguments and return values are round-tripped through CBOR, so
they must be CBOR-encodable (numbers, strings, symbols, arrays, hashes,
booleans, nil, and anything else `mruby-cbor` knows how to (de)serialise).

Exceptions raised inside the actor are re-raised in the caller.
