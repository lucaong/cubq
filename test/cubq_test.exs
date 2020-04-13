defmodule CubQTest do
  use ExUnit.Case
  doctest CubQ

  setup do
    tmp_dir = :os.cmd('mktemp -d') |> List.to_string() |> String.trim() |> String.to_charlist()
    cubdb_options = [data_dir: tmp_dir, auto_file_sync: false, auto_compact: false]

    {:ok, db} = CubDB.start_link(cubdb_options)

    on_exit(fn ->
      with {:ok, files} <- File.ls(tmp_dir) do
        for file <- files, do: File.rm(Path.join(tmp_dir, file))
      end

      :ok = File.rmdir(tmp_dir)
    end)

    {:ok, db: db}
  end

  test "queue operations", %{db: db} do
    {:ok, pid} = CubQ.start_link(db: db, queue: :my_queue)

    assert CubQ.dequeue(pid) == nil
    assert CubQ.peek_first(pid) == nil

    assert :ok = CubQ.enqueue(pid, :one)
    assert :ok = CubQ.enqueue(pid, :two)
    assert :ok = CubQ.enqueue(pid, :three)

    assert {:ok, :one} = CubQ.peek_first(pid)

    assert {:ok, :one} = CubQ.dequeue(pid)
    assert {:ok, :two} = CubQ.dequeue(pid)
    assert {:ok, :three} = CubQ.peek_first(pid)
    assert {:ok, :three} = CubQ.dequeue(pid)

    assert CubQ.dequeue(pid) == nil
    assert CubQ.peek_first(pid) == nil
  end

  test "stack operations", %{db: db} do
    {:ok, pid} = CubQ.start_link(db: db, queue: :my_queue)

    assert CubQ.pop(pid) == nil
    assert CubQ.peek_last(pid) == nil

    assert :ok = CubQ.push(pid, :one)
    assert :ok = CubQ.push(pid, :two)
    assert :ok = CubQ.push(pid, :three)

    assert {:ok, :three} = CubQ.peek_last(pid)

    assert {:ok, :three} = CubQ.pop(pid)
    assert {:ok, :two} = CubQ.pop(pid)
    assert {:ok, :one} = CubQ.peek_last(pid)
    assert {:ok, :one} = CubQ.pop(pid)

    assert CubQ.pop(pid) == nil
    assert CubQ.peek_last(pid) == nil
  end

  test "append and prepend", %{db: db} do
    {:ok, pid} = CubQ.start_link(db: db, queue: :my_queue)

    assert :ok = CubQ.append(pid, :one)
    assert :ok = CubQ.prepend(pid, :zero)
    assert :ok = CubQ.append(pid, :two)

    assert {:ok, :zero} = CubQ.peek_first(pid)
    assert {:ok, :two} = CubQ.peek_last(pid)

    assert {:ok, :zero} = CubQ.dequeue(pid)
    assert {:ok, :one} = CubQ.dequeue(pid)
    assert {:ok, :two} = CubQ.dequeue(pid)

    assert CubQ.dequeue(pid) == nil
    assert CubQ.peek_first(pid) == nil
  end
end
