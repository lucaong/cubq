defmodule CubQTest do
  use ExUnit.Case, async: true
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
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.enqueue(q2, "one")

    assert CubQ.dequeue(q) == nil
    assert CubQ.peek_first(q) == nil

    assert :ok = CubQ.enqueue(q, :one)
    assert :ok = CubQ.enqueue(q, :two)
    assert :ok = CubQ.enqueue(q, :three)

    assert {:ok, :one} = CubQ.peek_first(q)

    assert {:ok, :one} = CubQ.dequeue(q)
    assert {:ok, :two} = CubQ.dequeue(q)
    assert {:ok, :three} = CubQ.peek_first(q)
    assert {:ok, :three} = CubQ.dequeue(q)

    assert CubQ.dequeue(q) == nil
    assert CubQ.peek_first(q) == nil

    assert {:ok, "one"} = CubQ.peek_first(q2)
  end

  test "stack operations", %{db: db} do
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.push(q2, "one")

    assert CubQ.pop(q) == nil
    assert CubQ.peek_last(q) == nil

    assert :ok = CubQ.push(q, :one)
    assert :ok = CubQ.push(q, :two)
    assert :ok = CubQ.push(q, :three)

    assert {:ok, :three} = CubQ.peek_last(q)

    assert {:ok, :three} = CubQ.pop(q)
    assert {:ok, :two} = CubQ.pop(q)
    assert {:ok, :one} = CubQ.peek_last(q)
    assert {:ok, :one} = CubQ.pop(q)

    assert CubQ.pop(q) == nil
    assert CubQ.peek_last(q) == nil

    assert {:ok, "one"} = CubQ.peek_last(q2)
  end

  test "append and prepend", %{db: db} do
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.enqueue(q2, "one")

    assert :ok = CubQ.append(q, :one)
    assert :ok = CubQ.prepend(q, :zero)
    assert :ok = CubQ.append(q, :two)

    assert {:ok, :zero} = CubQ.peek_first(q)
    assert {:ok, :two} = CubQ.peek_last(q)

    assert {:ok, :zero} = CubQ.dequeue(q)
    assert {:ok, :one} = CubQ.dequeue(q)
    assert {:ok, :two} = CubQ.dequeue(q)

    assert CubQ.dequeue(q) == nil
    assert CubQ.peek_first(q) == nil

    assert {:ok, "one"} = CubQ.peek_first(q2)
  end

  test "delete_all/1 deletes all items in the queue leaving other entries unchanged", %{db: db} do
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.enqueue(q2, "one")

    assert :ok = CubQ.enqueue(q, :one)
    assert :ok = CubQ.enqueue(q, :two)
    assert :ok = CubQ.enqueue(q, :three)

    assert :ok = CubQ.delete_all(q, 2)
    assert CubQ.dequeue(q) == nil

    assert :ok = CubQ.delete_all(q)

    assert {:ok, "one"} = CubQ.peek_first(q2)
  end

  test "queue operations with explicit ack/nack", %{db: db} do
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.enqueue(q2, "one")
    assert :ok = CubQ.enqueue(q2, "two")
    assert _ = CubQ.dequeue_ack(q2, 10_000)

    assert :ok = CubQ.enqueue(q, :one)
    assert :ok = CubQ.enqueue(q, :two)
    assert :ok = CubQ.enqueue(q, :three)

    assert {:ok, :one} = CubQ.peek_first(q)

    assert {:ok, :one, ack_id} = CubQ.dequeue_ack(q)
    assert {:ok, :two} = CubQ.peek_first(q)

    assert :ok = CubQ.nack(q, ack_id)
    assert {:ok, :one} = CubQ.peek_first(q)

    assert {:ok, :one, ack_id} = CubQ.dequeue_ack(q)
    assert {:ok, :two} = CubQ.peek_first(q)

    assert :ok = CubQ.ack(q, ack_id)
    assert {:ok, :two} = CubQ.peek_first(q)

    assert {:ok, :two, _ack_id} = CubQ.dequeue_ack(q, 150)
    assert {:ok, :three, _ack_id} = CubQ.dequeue_ack(q, 100)

    Process.sleep(1000)
    assert {:ok, :two} = CubQ.peek_first(q)

    assert {:ok, "two"} = CubQ.peek_first(q2)
  end

  test "stack operations with explicit ack/nack", %{db: db} do
    {:ok, q} = CubQ.start_link(db: db, queue: :my_queue)
    {:ok, q2} = CubQ.start_link(db: db, queue: :my_other_queue)

    assert :ok = CubQ.push(q2, "one")
    assert :ok = CubQ.push(q2, "two")
    assert _ = CubQ.pop_ack(q2, 10_000)

    assert :ok = CubQ.push(q, :zero)
    assert :ok = CubQ.push(q, :one)
    assert :ok = CubQ.push(q, :two)

    assert {:ok, :two} = CubQ.peek_last(q)

    assert {:ok, :two, ack_id} = CubQ.pop_ack(q)
    assert {:ok, :one} = CubQ.peek_last(q)

    assert :ok = CubQ.nack(q, ack_id)
    assert {:ok, :two} = CubQ.peek_last(q)

    assert {:ok, :two, ack_id} = CubQ.pop_ack(q)
    assert {:ok, :one} = CubQ.peek_last(q)

    assert :ok = CubQ.ack(q, ack_id)
    assert {:ok, :one} = CubQ.peek_last(q)

    assert {:ok, :one, _ack_id} = CubQ.pop_ack(q, 150)
    assert {:ok, :zero, _ack_id} = CubQ.pop_ack(q, 100)

    Process.sleep(1000)
    assert {:ok, :one} = CubQ.peek_last(q)

    assert {:ok, "one"} = CubQ.peek_last(q2)
  end
end
