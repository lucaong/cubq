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
end
