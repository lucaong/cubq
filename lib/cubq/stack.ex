defmodule CubQ.Stack do
  @moduledoc false

  def pop(db, {queue, conditions}) do
    case CubDB.select(db, [{:reverse, true} | conditions]) do
      {:ok, [{{^queue, n}, value}]} when is_number(n) ->
        with :ok <- CubDB.delete(db, {queue, n}), do: {:ok, value}

      {:ok, []} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end

  def peek_last(db, {queue, conditions}) do
    case CubDB.select(db, [{:reverse, true} | conditions]) do
      {:ok, [{{^queue, n}, value}]} when is_number(n) ->
        {:ok, value}

      {:ok, []} ->
        nil

      {:error, error} ->
        {:error, error}
    end
  end
end
