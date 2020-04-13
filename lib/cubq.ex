defmodule CubQ do
  use GenServer
  alias CubQ.Queue
  alias CubQ.Stack

  @moduledoc """
  `CubQ` is a queue abstraction on top of `CubDB`. It implements persistent
  local (double-ended) queue and stack semantics.

  ## Usage

  `CubQ` is given a `CubDB` process and a queue identifier upon start:

  ```
  {:ok, db} = CubDB.start_link(data_dir: "my/data/directory")
  {:ok, pid} = CubQ.start_link(db: db, queue: :my_queue_id)
  ```

  ### Queues

  Queue semantics are implemented by the `enqueue/2` and `dequeue/1` functions:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, :one}

  CubQ.dequeue(pid)
  #=> {:ok, :two}

  # When there are no more elements in the queue, `dequeue/1` returns `nil`:

  CubQ.dequeue(pid)
  #=> nil
  ```

  Note that elements can be any Elixir (or Erlang) term:

  ```
  CubQ.enqueue(pid, %SomeStruct{foo: "bar"})
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, %SomeStruct{foo: "bar"}}
  ```

  The queue is actually double-ended, so elements can be prepended too:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.prepend(pid, :zero)
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, :zero}
  ```

  ### Stacks

  Stack semantics are implemented by the `push/2` and `pop/1` functions:

  ```
  CubQ.push(pid, :one)
  #=> :ok

  CubQ.push(pid, :two)
  #=> :ok

  CubQ.pop(pid)
  #=> {:ok, :two}

  CubQ.pop(pid)
  #=> {:ok, :one}

  # When there are no more elements in the stack, `pop/1` returns `nil`:

  CubQ.pop(pid)
  #=> nil
  ```

  ### Mixing things up

  As the underlying data structure used for stacks and queues is the same, queue
  and stack semantics can be mixed on the same queue.
  """

  defmodule State do
    @type t :: %CubQ.State{
            db: GenServer.server(),
            conditions: {term, Keyword.t()}
          }

    @enforce_keys [:db, :conditions]
    defstruct [:db, :conditions]
  end

  @gen_server_options [:name, :timeout, :debug, :spawn_opt, :hibernate_after]

  @type element :: term
  @type option :: {:db, GenServer.server()} | {:queue, term}

  @spec start_link([option | GenServer.option()]) :: GenServer.on_start()

  @doc """
  Starts a `CubQ` process linked to the calling process.

  The argument is a keyword list of options:

    - `db` (required): the `pid` (or name) of the `CubDB` process for storing
    the queue

    - `queue` (required): the identifier of the queue. It can be any Elixir
    term, but typically one would use an atom, like `:my_queue`

  `GenServer` options like `name` and `timeout` can also be given, and are
  forwarded to `GenServer.start_link/3` as the third argument.
  """
  def start_link(options) do
    {gen_server_options, options} = Keyword.split(options, @gen_server_options)

    GenServer.start_link(__MODULE__, options, gen_server_options)
  end

  @spec start([option | GenServer.option()]) :: GenServer.on_start()

  @doc """
  Starts a `CubQ` process without link.

  The argument is a keyword list of options, see `start_link/1` for details.
  """
  def start(options) do
    {gen_server_options, options} = Keyword.split(options, @gen_server_options)

    GenServer.start(__MODULE__, options, gen_server_options)
  end

  @spec enqueue(GenServer.server(), element) :: :ok | {:error, term}

  @doc """
  Inserts an element at the end of the queue.

  The element can be any Elixir (or Erlang) term.

  ## Example:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, :one}
  ```
  """
  def enqueue(pid, element) do
    GenServer.call(pid, {:enqueue, element})
  end

  @spec dequeue(GenServer.server()) :: {:ok, element} | nil | {:error, term}

  @doc """
  Removes an element from the beginning of the queue and returns it.

  It returns `{:ok, element}` if there are elements in the queue, `nil` if the
  queue is empty, and `{:error, cause}` on error.

  ## Example:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, :one}

  CubQ.dequeue(pid)
  #=> {:ok, :two}
  ```
  """
  def dequeue(pid) do
    GenServer.call(pid, :dequeue)
  end

  @spec peek_first(GenServer.server()) :: {:ok, element} | nil | {:error, term}

  @doc """
  Returns the element at the beginning of the queue without removing it.

  It returns `{:ok, element}` if there are elements in the queue, `nil` if the
  queue is empty, and `{:error, cause}` on error.

  ## Example:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  CubQ.peek_first(pid)
  #=> {:ok, :one}

  CubQ.dequeue(pid)
  #=> {:ok, :one}
  ```
  """
  def peek_first(pid) do
    GenServer.call(pid, :peek_first)
  end

  @spec append(GenServer.server(), element) :: :ok | {:error, term}

  @doc """
  Same as `enqueue/2`
  """
  def append(pid, element) do
    enqueue(pid, element)
  end

  @spec prepend(GenServer.server(), element) :: :ok | {:error, term}

  @doc """
  Inserts an element at the beginning of the queue.

  The element can be any Elixir (or Erlang) term.

  ## Example:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.prepend(pid, :zero)
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, :zero}

  CubQ.dequeue(pid)
  #=> {:ok, :one}
  ```
  """
  def prepend(pid, element) do
    GenServer.call(pid, {:prepend, element})
  end

  @spec push(GenServer.server(), element) :: :ok | {:error, term}

  @doc """
  Same as `enqueue/2`.

  Normally used together with `pop/1` for stack semantics.
  """
  def push(pid, element) do
    enqueue(pid, element)
  end

  @spec pop(GenServer.server()) :: {:ok, element} | nil | {:error, term}

  @doc """
  Removes an element from the end of the queue and returns it.

  It returns `{:ok, element}` if there are elements in the queue, `nil` if the
  queue is empty, and `{:error, cause}` on error.

  ## Example:

  ```
  CubQ.push(pid, :one)
  #=> :ok

  CubQ.push(pid, :two)
  #=> :ok

  CubQ.pop(pid)
  #=> {:ok, :two}

  CubQ.pop(pid)
  #=> {:ok, :one}
  ```
  """
  def pop(pid) do
    GenServer.call(pid, :pop)
  end

  @spec peek_last(GenServer.server()) :: {:ok, element} | nil | {:error, term}

  @doc """
  Returns the element at the end of the queue without removing it.

  It returns `{:ok, element}` if there are elements in the queue, `nil` if the
  queue is empty, and `{:error, cause}` on error.

  ## Example:

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  CubQ.peek_last(pid)
  #=> {:ok, :two}

  CubQ.pop(pid)
  #=> {:ok, :two}
  ```
  """
  def peek_last(pid) do
    GenServer.call(pid, :peek_last)
  end

  @spec delete_all(GenServer.server(), pos_integer) :: :ok | {:error, term}

  @doc """
  Deletes all elements from the queue.

  The elements are deleted in batches, and the size of the batch can be
  specified as the optional second argument.
  """
  def delete_all(pid, batch_size \\ 100) do
    GenServer.call(pid, {:delete_all, batch_size})
  end

  # GenServer callbacks

  @impl true

  def init(options) do
    db = Keyword.fetch!(options, :db)
    queue = Keyword.fetch!(options, :queue)
    conditions = {queue, select_conditions(queue)}
    {:ok, %State{db: db, conditions: conditions}}
  end

  @impl true

  def handle_call({:enqueue, element}, _from, state = %State{db: db, conditions: conditions}) do
    reply = Queue.enqueue(db, conditions, element)
    {:reply, reply, state}
  end

  def handle_call(:dequeue, _from, state = %State{db: db, conditions: conditions}) do
    reply = Queue.dequeue(db, conditions)
    {:reply, reply, state}
  end

  def handle_call(:peek_first, _from, state = %State{db: db, conditions: conditions}) do
    reply = Queue.peek_first(db, conditions)
    {:reply, reply, state}
  end

  def handle_call(:pop, _from, state = %State{db: db, conditions: conditions}) do
    reply = Stack.pop(db, conditions)
    {:reply, reply, state}
  end

  def handle_call(:peek_last, _from, state = %State{db: db, conditions: conditions}) do
    reply = Stack.peek_last(db, conditions)
    {:reply, reply, state}
  end

  def handle_call({:prepend, element}, _from, state = %State{db: db, conditions: conditions}) do
    reply = Queue.prepend(db, conditions, element)
    {:reply, reply, state}
  end

  def handle_call({:delete_all, batch_size}, _from, state = %State{db: db, conditions: conditions}) do
    reply = Queue.delete_all(db, conditions, batch_size)
    {:reply, reply, state}
  end

  defp select_conditions(queue) do
    [min_key: {queue, -1.0e32}, max_key: {queue, nil}, pipe: [take: 1]]
  end
end
