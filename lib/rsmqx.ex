defmodule Rsmqx do
  @moduledoc """
  Documentation for `Rsmqx`.
  """

  def create_queue(conn, queue_name, opts \\ []) do
    key = queue_key(queue_name)

    case do_create_queue(conn, key, opts) do
      :ok -> index_queue(conn, queue_name)
      error -> error
    end
  end

  defp do_create_queue(conn, key, opts) do
    pipeline =
      [
        ["hsetnx", key, :vt, opts[:vt] || 30],
        ["hsetnx", key, :delay, opts[:delay] || 0],
        ["hsetnx", key, :maxsize, opts[:maxsize] || 65536]
      ]

    Redix.transaction_pipeline(conn, pipeline)
    |> handle_result([1, 1, 1], :queue_exists)
  end

  defp index_queue(conn, queue_name) do
    Redix.command(conn, ["sadd", "rsmq:QUEUES", queue_name])
    |> handle_result(1, :queue_index_exists)
  end

  defp queue_key(queue_name), do: "rsmq:#{queue_name}:Q"

  defp handle_result({:error, error}, _, _), do: {:error, error}
  defp handle_result({:ok, resp}, expected, _) when resp == expected, do: :ok
  defp handle_result(_, _, error_message), do: {:error, error_message}
end
