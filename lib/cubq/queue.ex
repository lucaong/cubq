defmodule CubQ.Queue do
  @moduledoc false

  @typep key :: {term, number}
  @typep entry :: {key, CubQ.element()}
  @typep select_option ::
           {:min_key, key}
           | {:max_key, key}
           | {:reverse, boolean}
           | {:pipe, [{:take, pos_integer}]}

  @spec enqueue(GenServer.server(), term, CubQ.element()) :: :ok | {:error, term}

  def enqueue(db, queue, element) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, {{_queue, n}, _value}} ->
        CubDB.put(db, {queue, n + 1}, element)

      nil ->
        CubDB.put(db, {queue, 0}, element)

      other ->
        other
    end
  end

  @spec prepend(GenServer.server(), term, CubQ.element()) :: :ok | {:error, term}

  def prepend(db, queue, element) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {{_queue, n}, _value}} ->
        CubDB.put(db, {queue, n - 1}, element)

      nil ->
        CubDB.put(db, {queue, 0}, element)

      other ->
        other
    end
  end

  @spec dequeue(GenServer.server(), term) :: {:ok, CubQ.element()} | nil | {:error, term}

  def dequeue(db, queue) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {key, value}} ->
        with :ok <- CubDB.delete(db, key), do: {:ok, value}

      other ->
        other
    end
  end

  @spec pop(GenServer.server(), term) :: {:ok, CubQ.element()} | nil | {:error, term}

  def pop(db, queue) do
    conditions = select_conditions(queue)

    case get_entry(db, queue, [{:reverse, true} | conditions]) do
      {:ok, {key, value}} ->
        with :ok <- CubDB.delete(db, key), do: {:ok, value}

      other ->
        other
    end
  end

  @spec peek_first(GenServer.server(), term) :: {:ok, CubQ.element()} | nil | {:error, term}

  def peek_first(db, queue) do
    case get_entry(db, queue, select_conditions(queue)) do
      {:ok, {_key, value}} ->
        {:ok, value}

      other ->
        other
    end
  end

  @spec peek_last(GenServer.server(), term) :: {:ok, CubQ.element()} | nil | {:error, term}

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

      {:ok, elements} when is_list(elements) ->
        keys = Enum.map(elements, fn {key, _value} -> key end)

        with :ok <- CubDB.delete_multi(db, keys),
             do: delete_all(db, queue, batch_size)

      {:error, error} ->
        {:error, error}
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
end
