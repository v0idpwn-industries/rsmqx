defmodule RsmqxTest do
  use ExUnit.Case
  import UUID
  doctest Rsmqx

  setup_all do
    {:ok, conn} = Redix.start_link(sync_connect: true)
    Redix.command!(conn, ["FLUSHALL"])
    :ok = Redix.stop(conn)
    :ok
  end

  setup do
    {:ok, conn} = Redix.start_link()
    name = uuid4()

    Rsmqx.create_queue(conn, name)

    %{conn: conn, queue_name: name}
  end

  describe "create_queue/3" do
    test "success without opts", %{conn: conn} do
      name = uuid4()

      assert Rsmqx.create_queue(conn, name) == :ok

      {:ok, queues} = Redix.command(conn, ["smembers", "rsmq:QUEUES"])

      assert name in queues

      assert get_all(conn, "rsmq:#{name}:Q") == [
               "vt",
               "30",
               "delay",
               "0",
               "maxsize",
               "65536"
             ]
    end

    test "success with opts", %{conn: conn} do
      name = uuid4()

      assert Rsmqx.create_queue(conn, name, vt: 40, delay: 10, maxsize: 10400) == :ok

      {:ok, queues} = Redix.command(conn, ["smembers", "rsmq:QUEUES"])

      assert name in queues

      assert get_all(conn, "rsmq:#{name}:Q") == [
               "vt",
               "40",
               "delay",
               "10",
               "maxsize",
               "10400"
             ]
    end

    test "fail when queue exists", %{conn: conn, queue_name: name} do
      assert Rsmqx.create_queue(conn, name) == {:error, :queue_exists}
    end

    test "fail when params are invalid", %{conn: conn} do
      name = 1234
      params = [vt: 30.5, delay: "asdf", maxsize: %{}]

      assert Rsmqx.create_queue(conn, name, params) ==
               {:error,
                %{
                  message: :invalid_params,
                  errors: [
                    queue_name: "must be string",
                    vt: "must be integer",
                    delay: "must be integer",
                    maxsize: "must be integer"
                  ]
                }}

      {:ok, queues} = Redix.command(conn, ["smembers", "rsmq:QUEUES"])

      assert name not in queues
      assert get_all(conn, "rsmq:#{name}:Q") == []
    end
  end

  describe "list_queues/1" do
    test "success", %{conn: conn, queue_name: name} do
      queues = Rsmqx.list_queues(conn)

      assert name in queues
    end
  end

  describe "delete_queue/2" do
    test "success", %{conn: conn, queue_name: name} do
      Rsmqx.send_message(conn, name, "test")

      assert match?([_], get_queue_messages(conn, name))

      :ok = Rsmqx.delete_queue(conn, name)

      {:ok, queues} = Redix.command(conn, ["smembers", "rsmq:QUEUES"])

      refute name in queues
      assert match?([], get_queue_messages(conn, name))
    end

    test "fail when queue don't exists", %{conn: conn, queue_name: name} do
      Rsmqx.delete_queue(conn, name)
      assert {:error, :queue_not_found} == Rsmqx.delete_queue(conn, name)
    end
  end

  describe "send_message/4" do
    test "successfully includes in queue", %{conn: conn, queue_name: name} do
      message = "hello world - #{uuid4()}"
      {:ok, id} = Rsmqx.send_message(conn, name, message)

      assert id in get_queue_messages(conn, name)
      assert [message] == get_member(conn, data_key(name), id)
    end

    test "delay properly affects order", %{conn: conn, queue_name: name} do
      {:ok, id2} = Rsmqx.send_message(conn, name, "test", delay: 10)
      {:ok, id3} = Rsmqx.send_message(conn, name, "test", delay: 20)
      {:ok, id1} = Rsmqx.send_message(conn, name, "test")

      assert get_queue_messages(conn, name) == [id1, id2, id3]
    end

    test "maxsize set as -1 works as infinite size", %{conn: conn} do
      name = uuid4()

      Rsmqx.create_queue(conn, name, maxsize: -1)

      message = get_really_big_message()

      {:ok, id} = Rsmqx.send_message(conn, name, message)

      assert id in get_queue_messages(conn, name)
      assert [message] == get_member(conn, data_key(name), id)
    end

    test "fail when queue don't exists", %{conn: conn} do
      assert {:error, :queue_not_found} == Rsmqx.send_message(conn, "asdf", "test")
    end

    test "fail when message is bigger than maxsize", %{conn: conn} do
      name = uuid4()

      Rsmqx.create_queue(conn, name, maxsize: 2)

      message = "123"

      assert {:error, :message_too_long} == Rsmqx.send_message(conn, name, message)
    end
  end

  describe "receive_message/2" do
    test "basic success cases", %{conn: conn, queue_name: name} do

      message1 = "hello world"
      message2 = "test"
      message3 = "more test"

      {:ok, id1} = Rsmqx.send_message(conn, name, message1, vt: 1000)
      {:ok, id2} = Rsmqx.send_message(conn, name, message2)
      {:ok, id3} = Rsmqx.send_message(conn, name, message3)

      :persistent_term.erase(:rsmqx_receive_message_script)

      assert match?({:ok, [^id1, ^message1, _, _]}, Rsmqx.receive_message(conn, name))
      assert match?({:ok, [^id2, ^message2, _, _]}, Rsmqx.receive_message(conn, name))
      assert match?({:ok, [^id3, ^message3, _, _]}, Rsmqx.receive_message(conn, name))
      assert {:ok, []} == Rsmqx.receive_message(conn, name)

      assert :persistent_term.get(:rsmqx_receive_message_script, nil)
    end

    test "delay works properly", %{conn: conn, queue_name: name} do
      message1 = "hello world"
      message2 = "test"
      message3 = "more test"

      {:ok, id1} = Rsmqx.send_message(conn, name, message1)
      {:ok, _id2} = Rsmqx.send_message(conn, name, message2, delay: 1)
      {:ok, id3} = Rsmqx.send_message(conn, name, message3)

      assert match?({:ok, [^id1, ^message1, _, _]}, Rsmqx.receive_message(conn, name))
      assert match?({:ok, [^id3, ^message3, _, _]}, Rsmqx.receive_message(conn, name))
      assert {:ok, []} == Rsmqx.receive_message(conn, name)
    end

    test "script will be reloaded in case of flush or restart", %{conn: conn, queue_name: name} do
      {:ok, _} = Rsmqx.receive_message(conn, name)

      assert hash = :persistent_term.get(:rsmqx_receive_message_script, nil)
      assert {:ok, [1]} == Redix.command(conn, ["script", "exists", hash])

      # flush script
      Redix.command(conn, ["script", "flush"])

      refute {:ok, [1]} == Redix.command(conn, ["script", "exists", hash])

      {:ok, _} = Rsmqx.receive_message(conn, name)

      assert {:ok, [1]} == Redix.command(conn, ["script", "exists", hash])
    end

    test "failure when queue don't exists", %{conn: conn} do
        name = "not_found"
        assert {:error, :queue_not_found} == Rsmqx.receive_message(conn, name)
    end
  end

  describe "delete_message/3" do
    test "success case", %{conn: conn, queue_name: name} do
      message = "test"

      {:ok, id} = Rsmqx.send_message(conn, name, message)

      assert id in get_queue_messages(conn, name)
      assert [message] == get_member(conn, data_key(name), id)

      assert :ok == Rsmqx.delete_message(conn, name, id)

      refute id in get_queue_messages(conn, name)
      refute [message] == get_member(conn, data_key(name), id)
    end

    test "fail when queue don't exists", %{conn: conn} do
      name = "not_found"
      id = "whatever"

      assert {:error, :message_not_found} == Rsmqx.delete_message(conn, name, id)
    end

    test "fail when queue exists but message don't", %{conn: conn, queue_name: name} do
      id = "not_found"

      assert {:error, :message_not_found} == Rsmqx.delete_message(conn, name, id)
    end
  end

  defp get_really_big_message, do: File.read!("test/test_helper/big_message.txt")

  defp get_member(conn, set, member), do: Redix.command!(conn, ["hmget", set, member])
  defp get_all(conn, name), do: Redix.command!(conn, ["hgetall", name])

  defp get_queue_messages(conn, name), do: Redix.command!(conn, ["zrange", "rsmq:#{name}", 0, -1])

  defp data_key(queue_name), do: "rsmq:#{queue_name}:Q"
end
