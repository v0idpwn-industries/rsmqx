defmodule RsmqxTest do
  use ExUnit.Case
  doctest Rsmqx

  setup_all do
    {:ok, conn} = Redix.start_link(sync_connect: true)
    Redix.command!(conn, ["FLUSHALL"])
    :ok = Redix.stop(conn)
    :ok
  end

  setup do
    {:ok, conn} = Redix.start_link()
    name = generate_name()

    Rsmqx.create_queue(conn, name)

    %{conn: conn, queue_name: name}
  end

  describe "create_queue/3" do
    test "success without opts", %{conn: conn} do
      name = generate_name()

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
      name = generate_name()

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

  defp generate_name, do: "q-#{Enum.random(100_000..999_999)}"

  defp get_all(conn, name), do: Redix.command!(conn, ["hgetall", name])
end
