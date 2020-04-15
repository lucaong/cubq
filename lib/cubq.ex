defmodule CubQ do
  use GenServer
  alias CubQ.Queue

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

  # When there are no more items in the queue, `dequeue/1` returns `nil`:

  CubQ.dequeue(pid)
  #=> nil
  ```

  Note that items can be any Elixir (or Erlang) term:

  ```
  CubQ.enqueue(pid, %SomeStruct{foo: "bar"})
  #=> :ok

  CubQ.dequeue(pid)
  #=> {:ok, %SomeStruct{foo: "bar"}}
  ```

  The queue is actually double-ended, so items can be prepended too:

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

  # When there are no more items in the stack, `pop/1` returns `nil`:

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
            queue: term,
            pending_acks: %{}
          }

    @enforce_keys [:db, :queue]
    defstruct [:db, :queue, pending_acks: %{}]
  end

  @gen_server_options [:name, :timeout, :debug, :spawn_opt, :hibernate_after]

  @type item :: term
  @opaque ack_id :: {term, reference, number, :start | :end}
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

  @spec enqueue(GenServer.server(), item) :: :ok | {:error, term}

  @doc """
  Inserts an item at the end of the queue.

  The item can be any Elixir (or Erlang) term.

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
  def enqueue(pid, item) do
    GenServer.call(pid, {:enqueue, item})
  end

  @spec dequeue(GenServer.server()) :: {:ok, item} | nil | {:error, term}

  @doc """
  Removes an item from the beginning of the queue and returns it.

  It returns `{:ok, item}` if there are items in the queue, `nil` if the
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

  @spec dequeue_ack(GenServer.server(), timeout) :: {:ok, item, ack_id} | nil | {:error, term}

  @doc """
  Removes an item from the beginning of the queue and returns it, expecting a
  manual confirmation of its successful processing with `ack/2`. If `ack/2` is
  not called before `timeout`, the item is put back at the beginning of the
  queue.

  The `dequeue_ack/2` function is useful when implementing "at least once"
  semantics, especially when more than one consumer takes items from the same
  queue, to ensure that each item is successfully consumed before being
  discarded.

  The problem is the following: if a consumer took an item with `dequeue/1` and
  then crashes before processing it, the item would be lost. With
  `dequeue_ack/2` instead, the item is not immediately removed, but instead
  atomically transfered to a staging storage. If the consumer successfully
  processes the item, it can call `ack/2` (acknowledgement) to confirm the
  success, after which the item is discarded. If `ack/2` is not called within
  the `timeout` (expressed in milliseconds, 5000 by default), the item is
  automatically put back to the queue, so it can be dequeued again by a
  consumer. If the consumer wants to put back the item to the queue immediately,
  it can also call `nack/2` (negative acknowledgement) explicitly.

  The return value of `dequeue_ack/2` is a 3-tuple: `{:ok, item, ack_id}`.
  The `ack_id` is the argument that must be passed to `ack/2` or `nack/2` to
  confirm the successful (or insuccessful) consumption of the item.

  Note that `dequeue_ack/2` performs its operation in an atomic and durable way,
  so even if the `CubQ` process crashes, after restarting it will still
  re-insert the items pending acknowledgement in the queue after the timeout
  elapses.  After restarting though, the timeouts are also restarted, so the
  effective time before the item goes back to the queue can be larger than the
  original timeout.

  In case of timeout or negative acknowledgement, the item is put back in the
  queue from the start, so while global ordering cannot be enforced in case of
  items being put back to the queue, the items will be ready to be dequeued
  again immediately after being back to the queue.

  ## Example

  ```
  CubQ.enqueue(pid, :one)
  #=> :ok

  CubQ.enqueue(pid, :two)
  #=> :ok

  {:ok, item, ack_id} = CubQ.dequeue_ack(pid, 3000)
  #=> {:ok, :one, ack_id}

  # More items can be now taken from the queue
  CubQ.dequeue(pid)
  #=> {:ok, :two}

  # If 3 seconds elapse without `ack` being called, or `nack` is called,
  # the item `:one` would be put back to the queue, so it can be dequeued
  # again:
  CubQ.nack(pid, ack_id)
  #=> :ok

  {:ok, item, ack_id} = CubQ.dequeue_ack(pid, 3000)
  #=> {:ok, :one, ack_id}

  # When successful consumption is confirmed by calling `ack`, the item
  # is finally discarded and won't be put back in the queue anymore:
  CubQ.ack(pid, ack_id)
  #=> :ok
  ```
  """
  def dequeue_ack(pid, timeout \\ 5000) do
    GenServer.call(pid, {:dequeue_ack, timeout})
  end

  @spec pop_ack(GenServer.server(), timeout) :: {:ok, item, ack_id} | nil | {:error, term}

  @doc """
  Removes an item from the end of the queue and returns it, expecting a manual
  confirmation of its successful processing with `ack/2`. If `ack/2` is not
  called before `timeout`, the item is put back at the end of the queue.

  See the documentation for `dequeue_ack/2` for more details: the `pop_ack/2`
  function works in the same way as `dequeue_ack/2`, but with stack semantics
  instead of queue semantics.
  """
  def pop_ack(pid, timeout \\ 5000) do
    GenServer.call(pid, {:pop_ack, timeout})
  end

  @spec ack(GenServer.server(), ack_id) :: :ok | {:error, term}

  @doc """
  Positively acknowledges the successful consumption of an item taken with
  `dequeue_ack/2` or `pop_ack/2`.

  See the documentation for `dequeue_ack/2` for more details.
  """
  def ack(pid, ack_id) do
    GenServer.call(pid, {:ack, ack_id})
  end

  @spec nack(GenServer.server(), ack_id) :: :ok | {:error, term}

  @doc """
  Negatively acknowledges an item taken with `dequeue_ack/2` or `pop_ack/2`,
  causing it to be put back in the queue.

  See the documentation for `dequeue_ack/2` for more details.
  """
  def nack(pid, ack_id) do
    GenServer.call(pid, {:nack, ack_id})
  end

  @spec peek_first(GenServer.server()) :: {:ok, item} | nil | {:error, term}

  @doc """
  Returns the item at the beginning of the queue without removing it.

  It returns `{:ok, item}` if there are items in the queue, `nil` if the
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

  @spec append(GenServer.server(), item) :: :ok | {:error, term}

  @doc """
  Same as `enqueue/2`
  """
  def append(pid, item) do
    enqueue(pid, item)
  end

  @spec prepend(GenServer.server(), item) :: :ok | {:error, term}

  @doc """
  Inserts an item at the beginning of the queue.

  The item can be any Elixir (or Erlang) term.

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
  def prepend(pid, item) do
    GenServer.call(pid, {:prepend, item})
  end

  @spec push(GenServer.server(), item) :: :ok | {:error, term}

  @doc """
  Same as `enqueue/2`.

  Normally used together with `pop/1` for stack semantics.
  """
  def push(pid, item) do
    enqueue(pid, item)
  end

  @spec pop(GenServer.server()) :: {:ok, item} | nil | {:error, term}

  @doc """
  Removes an item from the end of the queue and returns it.

  It returns `{:ok, item}` if there are items in the queue, `nil` if the
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

  @spec peek_last(GenServer.server()) :: {:ok, item} | nil | {:error, term}

  @doc """
  Returns the item at the end of the queue without removing it.

  It returns `{:ok, item}` if there are items in the queue, `nil` if the
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
  Deletes all items from the queue.

  The items are deleted in batches, and the size of the batch can be
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
    {:ok, %State{db: db, queue: queue}, {:continue, nil}}
  end

  @impl true

  def handle_continue(_continue, state = %State{db: db, queue: queue}) do
    pending_acks =
      Enum.reduce(Queue.get_pending_acks!(db, queue), %{}, fn {key, {_, timeout}}, map ->
        Map.put(map, key, schedule_ack_timeout(key, timeout))
      end)

    {:noreply, %State{state | pending_acks: pending_acks}}
  end

  @impl true

  def handle_call({:enqueue, item}, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.enqueue(db, queue, item)
    {:reply, reply, state}
  end

  def handle_call(:dequeue, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.dequeue(db, queue)
    {:reply, reply, state}
  end

  def handle_call(:peek_first, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.peek_first(db, queue)
    {:reply, reply, state}
  end

  def handle_call(:pop, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.pop(db, queue)
    {:reply, reply, state}
  end

  def handle_call(:peek_last, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.peek_last(db, queue)
    {:reply, reply, state}
  end

  def handle_call({:prepend, item}, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.prepend(db, queue, item)
    {:reply, reply, state}
  end

  def handle_call({:delete_all, batch_size}, _from, state = %State{db: db, queue: queue}) do
    reply = Queue.delete_all(db, queue, batch_size)
    {:reply, reply, state}
  end

  def handle_call({:dequeue_ack, timeout}, _from, state) do
    %State{db: db, queue: queue, pending_acks: pending_acks} = state
    reply = Queue.dequeue_ack(db, queue, timeout)

    state =
      case reply do
        {:ok, _, ack_id} ->
          ref = schedule_ack_timeout(ack_id, timeout)
          %State{state | pending_acks: Map.put(pending_acks, ack_id, ref)}

        _ ->
          state
      end

    {:reply, reply, state}
  end

  def handle_call({:pop_ack, timeout}, _from, state) do
    %State{db: db, queue: queue, pending_acks: pending_acks} = state
    reply = Queue.pop_ack(db, queue, timeout)

    state =
      case reply do
        {:ok, _, ack_id} ->
          ref = schedule_ack_timeout(ack_id, timeout)
          %State{state | pending_acks: Map.put(pending_acks, ack_id, ref)}

        _ ->
          state
      end

    {:reply, reply, state}
  end

  def handle_call({:ack, ack_id}, _from, state) do
    %State{db: db, queue: queue, pending_acks: pending_acks} = state

    with :ok <- Queue.ack(db, queue, ack_id) do
      case Map.pop(pending_acks, ack_id) do
        {nil, _} ->
          {:reply, {:error, :not_found}, state}

        {timer, pending_acks} ->
          Process.cancel_timer(timer)
          {:reply, :ok, %State{state | pending_acks: pending_acks}}
      end
    else
      reply -> {:reply, reply, state}
    end
  end

  def handle_call({:nack, ack_id}, _from, state) do
    %State{db: db, queue: queue, pending_acks: pending_acks} = state

    with :ok <- Queue.nack(db, queue, ack_id) do
      case Map.pop(pending_acks, ack_id) do
        {nil, _} ->
          {:reply, {:error, :not_found}, state}

        {timer, pending_acks} ->
          Process.cancel_timer(timer)
          {:reply, :ok, %State{state | pending_acks: pending_acks}}
      end
    else
      reply -> {:reply, reply, state}
    end
  end

  @impl true

  def handle_info({:ack_timeout, ack_id}, state) do
    %State{db: db, queue: queue, pending_acks: pending_acks} = state
    :ok = Queue.nack(db, queue, ack_id)
    {:noreply, %State{state | pending_acks: Map.delete(pending_acks, ack_id)}}
  end

  @spec schedule_ack_timeout(ack_id, timeout) :: reference

  defp schedule_ack_timeout(ack_id, timeout) do
    Process.send_after(self(), {:ack_timeout, ack_id}, timeout)
  end
end
