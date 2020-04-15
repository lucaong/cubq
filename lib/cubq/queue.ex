defmodule CubQ.Queue do
  @moduledoc false

  @typep key :: {term, number}
  @typep entry :: {key, CubQ.item()}
  @typep select_option ::
           {:min_key, key}
           | {:max_key, key}
           | {:reverse, boolean}
           | {:pipe, [{:take, pos_integer}]}

  @spec enqueue(GenServer.server(), term, CubQ.item()) :: :ok | {:error, term}

  def enqueue(db, queue, item) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, {{_queue, n}, _value}} ->
        CubDB.put(db, {queue, n + 1}, item)

      nil ->
        CubDB.put(db, {queue, 0}, item)

      other ->
        other
    end
  end

  @spec prepend(GenServer.server(), term, CubQ.item()) :: :ok | {:error, term}

  def prepend(db, queue, item) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {{_queue, n}, _value}} ->
        CubDB.put(db, {queue, n - 1}, item)

      nil ->
        CubDB.put(db, {queue, 0}, item)

      other ->
        other
    end
  end

  @spec dequeue(GenServer.server(), term) :: {:ok, CubQ.item()} | nil | {:error, term}

  def dequeue(db, queue) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {key, value}} ->
        with :ok <- CubDB.delete(db, key), do: {:ok, value}

      other ->
        other
    end
  end

  @spec pop(GenServer.server(), term) :: {:ok, CubQ.item()} | nil | {:error, term}

  def pop(db, queue) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, {key, value}} ->
        with :ok <- CubDB.delete(db, key), do: {:ok, value}

      other ->
        other
    end
  end

  @spec peek_first(GenServer.server(), term) :: {:ok, CubQ.item()} | nil | {:error, term}

  def peek_first(db, queue) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {_key, value}} ->
        {:ok, value}

      other ->
        other
    end
  end

  @spec peek_last(GenServer.server(), term) :: {:ok, CubQ.item()} | nil | {:error, term}

  def peek_last(db, queue) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, {_key, value}} ->
        {:ok, value}

      other ->
        other
    end
  end

  @spec delete_all(GenServer.server(), term, pos_integer) :: :ok | {:error, term}

  def delete_all(db, queue, batch_size \\ 100) do
    conditions = select_conditions(queue)
    pipe = Keyword.get(conditions, :pipe, []) |> Keyword.put(:take, batch_size)
    batch_conditions = Keyword.put(conditions, :pipe, pipe)

    case CubDB.select(db, batch_conditions) do
      {:ok, []} ->
        :ok

      {:ok, items} when is_list(items) ->
        keys = Enum.map(items, fn {key, _value} -> key end)

        with :ok <- CubDB.delete_multi(db, keys),
             do: delete_all(db, queue, batch_size)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec dequeue_ack(GenServer.server(), term, timeout) ::
          {:ok, CubQ.item(), CubQ.ack_id()} | nil | {:error, term}

  def dequeue_ack(db, queue, timeout) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, entry} ->
        stage_item(db, queue, entry, timeout, :start)

      other ->
        other
    end
  end

  @spec pop_ack(GenServer.server(), term, timeout) ::
          {:ok, CubQ.item(), CubQ.ack_id()} | nil | {:error, term}

  def pop_ack(db, queue, timeout) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, entry} ->
        stage_item(db, queue, entry, timeout, :end)

      other ->
        other
    end
  end

  @spec ack(GenServer.server(), term, CubQ.ack_id()) :: :ok | {:error, term}

  def ack(db, _queue, ack_id) do
    CubDB.delete(db, ack_id)
  end

  @spec nack(GenServer.server(), term, CubQ.ack_id()) :: :ok | {:error, term}

  def nack(db, queue, ack_id) do
    # item can be requeued at the start or at the end of the queue
    {conditions, increment} = case ack_id do
      {_, _, _, :end} ->
        {[{:reverse, true} | select_conditions(queue)], 1}

      _ ->
        {select_conditions(queue), -1}
    end

    case get_entry(db, queue, conditions) do
      {:ok, {{_queue, n}, _value}} ->
        with {:ok, nil} <- commit_nack(db, queue, ack_id, n + increment), do: :ok

      nil ->
        with {:ok, nil} <- commit_nack(db, queue, ack_id, 0), do: :ok

      other ->
        other
    end
  end

  @spec get_pending_acks!(GenServer.server(), term) :: [
          {CubQ.ack_id(), {CubQ.item(), timeout}}
        ]

  def get_pending_acks!(db, queue) do
    case CubDB.select(db,
      min_key: {queue, nil, 0, 0},
      max_key: {queue, [], nil, []}
    ) do
      {:ok, entries} -> entries
      {:error, error} -> raise(error)
    end
  end

  @spec select_conditions(term) :: [select_option]

  defp select_conditions(queue) do
    [min_key: {queue, -1.0e32}, max_key: {queue, nil}, pipe: [take: 1]]
  end

  @spec get_entry(GenServer.server(), term, [select_option]) ::
          {:ok, entry} | nil | {:error, term}

  defp get_entry(db, queue, conditions) do
    case CubDB.select(db, conditions) do
      {:ok, [entry = {{^queue, n}, _value}]} when is_number(n) ->
        {:ok, entry}

      {:ok, []} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end

  @spec stage_item(GenServer.server(), term, entry, timeout, :start | :end) ::
          {:ok, CubQ.item(), term} | nil | {:error, term}

  defp stage_item(db, queue, entry, timeout, requeue_pos) do
    {key, item} = entry
    ack_id = {queue, make_ref(), :rand.uniform_real(), requeue_pos}

    case CubDB.get_and_update_multi(db, [], fn _ ->
           {nil, %{ack_id => {item, timeout}}, [key]}
         end) do
      {:ok, nil} ->
        {:ok, item, ack_id}

      other ->
        other
    end
  end

  @spec commit_nack(GenServer.server(), term, CubQ.ack_id(), number) ::
          {:ok, term} | {:error, term}

  defp commit_nack(db, queue, ack_id, n) do
    CubDB.get_and_update_multi(db, [ack_id], fn
      %{^ack_id => {item, _timeout}} ->
        {nil, %{{queue, n} => item}, [ack_id]}

      _ ->
        {nil, %{}, []}
    end)
  end
end
