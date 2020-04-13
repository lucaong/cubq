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

### Queue

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

# When there are no more elements in the queue, `dequeue` returns `nil`:

CubQ.dequeue(pid)
#=> nil
```

Note that elements can be any Elixir (or Erlang) term:

```elixir
CubQ.enqueue(pid, %SomeStruct{foo: "bar"})
#=> :ok

CubQ.dequeue(pid)
#=> {:ok, %SomeStruct{foo: "bar"}}
```

The queue is actually double-ended, so elements can be prepended too:

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

# When there are no more elements in the stack, `pop` returns `nil`:

CubQ.pop(pid)
#=> nil
```

### Mixing things up

As the underlying data structure used for stacks and queues is the same, queue
and stack semantics can be mixed on the same queue.

## Installation

The package can be installed by adding `cubq` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:cubq, "~> 0.2.0"}
  ]
end
```

Documentation can be generated with
[ExDoc](https://github.com/elixir-lang/ex_doc) and published on
[HexDocs](https://hexdocs.pm). The docs can be found at
[https://hexdocs.pm/cubq](https://hexdocs.pm/cubq).
