defmodule CubQ.Queue do
  @moduledoc false

  def enqueue(db, {queue, conditions}, element) do
    case CubDB.select(db, [{:reverse, true} | conditions]) do
      {:ok, [{{^queue, n}, _value}]} when is_number(n) ->
        CubDB.put(db, {queue, n + 1}, element)

      {:ok, []} ->
        CubDB.put(db, {queue, 0}, element)

      {:error, error} ->
        {:error, error}
    end
  end

  def prepend(db, {queue, conditions}, element) do
    case CubDB.select(db, conditions) do
      {:ok, [{{^queue, n}, _value}]} when is_number(n) ->
        CubDB.put(db, {queue, n - 1}, element)

      {:ok, []} ->
        CubDB.put(db, {queue, 0}, element)

      {:error, error} ->
        {:error, error}
    end
  end

  def dequeue(db, {queue, conditions}) do
    case CubDB.select(db, conditions) do
      {:ok, [{{^queue, n}, value}]} when is_number(n) ->
        with :ok <- CubDB.delete(db, {queue, n}), do: {:ok, value}

      {:ok, []} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end

  def peek_first(db, {queue, conditions}) do
    case CubDB.select(db, conditions) do
      {:ok, [{{^queue, n}, value}]} when is_number(n) ->
        {:ok, value}

      {:ok, []} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end

  def delete_all(db, {queue, conditions}, batch_size \\ 100) do
    pipe = Keyword.get(conditions, :pipe, []) |> Keyword.put(:take, batch_size)
    batch_conditions = Keyword.put(conditions, :pipe, pipe)

    case CubDB.select(db, batch_conditions) do
      {:ok, []} ->
        :ok

      {:ok, elements} when is_list(elements) ->
        keys = Enum.map(elements, fn {key, _value} -> key end)

        with :ok <- CubDB.delete_multi(db, keys),
             do: delete_all(db, {queue, conditions}, batch_size)

      {:error, error} ->
        {:error, error}
    end
  end
end
