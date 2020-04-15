[![Hex Version](https://img.shields.io/hexpm/v/cubq.svg)](https://hex.pm/packages/cubq) [![docs](https://img.shields.io/badge/docs-hexpm-blue.svg)](https://hexdocs.pm/cubq/)

# CubQ

An embedded queue and stack abstraction in Elixir on top of
[CubDB](https://github.com/lucaong/cubdb).

It implements persistent local (double-ended) queue and stack semantics.

## Usage

`CubQ` is given a `CubDB` process and a queue identifier upon start:

```elixir
{:ok, db} = CubDB.start_link(data_dir: "my/data/directory")
{:ok, pid} = CubQ.start_link(db: db, queue: :my_queue_id)
```

### Queues

Queue semantics are implemented by the `enqueue` and `dequeue` functions:

```elixir
CubQ.enqueue(pid, :one)
#=> :ok

CubQ.enqueue(pid, :two)
#=> :ok

CubQ.dequeue(pid)
#=> {:ok, :one}

CubQ.dequeue(pid)
#=> {:ok, :two}

# When there are no more items in the queue, `dequeue` returns `nil`:

CubQ.dequeue(pid)
#=> nil
```

Note that items can be any Elixir (or Erlang) term:

```elixir
CubQ.enqueue(pid, %SomeStruct{foo: "bar"})
#=> :ok

CubQ.dequeue(pid)
#=> {:ok, %SomeStruct{foo: "bar"}}
```

The queue is actually double-ended, so items can be prepended too:

```elixir
CubQ.enqueue(pid, :one)
#=> :ok

CubQ.prepend(pid, :zero)
#=> :ok

CubQ.dequeue(pid)
#=> {:ok, :zero}
```

### Stacks

Stack semantics are implemented by the `push` and `pop` functions:

```elixir
CubQ.push(pid, :one)
#=> :ok

CubQ.push(pid, :two)
#=> :ok

CubQ.pop(pid)
#=> {:ok, :two}

CubQ.pop(pid)
#=> {:ok, :one}

# When there are no more items in the stack, `pop` returns `nil`:

CubQ.pop(pid)
#=> nil
```

### Mixing things up

As the underlying data structure used for stacks and queues is the same, queue
and stack semantics can be mixed on the same queue.

### At-least-once semantics

When multiple consumers are taking items from a queue, and "at least once"
semantics are required, the `dequeue_ack` and `pop_ack` functions allow to
explicitly acknowledge the successful consumption of an item, or else put it
back in the queue after a given timeout elapses:

```elixir
CubQ.enqueue(pid, :one)
#=> :ok

CubQ.enqueue(pid, :two)
#=> :ok

{:ok, item, ack_id} = CubQ.dequeue_ack(pid, 3000)
#=> {:ok, :one, ack_id}

# If 3 seconds elapse without `ack` being called, or if `nack` is called, the
# item `:one` is put back to the queue, so it can be consumed again.

# When successful consumption is confirmed by calling `ack`, the item
# is finally discarded and won't be put back in the queue anymore:
CubQ.ack(pid, ack_id)
#=> :ok
```

## Installation

The package can be installed by adding `cubq` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:cubq, "~> 0.3.0"}
  ]
end
```

Documentation can be generated with
[ExDoc](https://github.com/elixir-lang/ex_doc) and published on
[HexDocs](https://hexdocs.pm). The docs can be found at
[https://hexdocs.pm/cubq](https://hexdocs.pm/cubq).
