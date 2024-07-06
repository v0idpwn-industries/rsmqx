defmodule Rsmqx do
  @moduledoc """
  Documentation for `Rsmqx`.
  """

  @queue_indexator "rsmq:QUEUES"

  @doc """
  Create a queue for messages
  """
  def create_queue(conn, queue_name, opts \\ []) do
    key = queue_key(queue_name)

    with :ok <- validate_params([{:queue_name, queue_name} | opts]),
         :ok <- do_create_queue(conn, key, opts),
         :ok <- index_queue(conn, queue_name) do
      :ok
    else
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
    Redix.command(conn, ["sadd", @queue_indexator, queue_name])
    |> handle_result(1, :queue_index_exists)
  end

  @doc """
  List all queues
  """
  def list_queues(conn), do: Redix.command(conn, ["smembers", @queue_indexator])

  def get_queue(conn, queue_name, create_id? \\ false) do
    key = queue_key(queue_name)

    [
      ["hmget", key, "vt", "delay", "maxsize"],
      ["time"]
    ]
    |> then(&Redix.transaction_pipeline(conn, &1))
    |> case do
      {:ok, [[nil, _, _], _]} ->
        {:error, :queue_not_found}

      {:ok, [[vt, delay, maxsize], [seconds, microseconds]]} ->
        timestamp = create_timestamp(seconds, microseconds)

        %{
          vt: vt,
          delay: delay,
          maxsize: maxsize,
          ts: timestamp
        }
        |> maybe_add_id(create_id?, seconds, microseconds)
        |> then(&{:ok, &1})

      error ->
        error
    end
  end

  defp create_timestamp(seconds, microseconds) do
    microseconds
    |> String.pad_leading(6, "0")
    |> String.slice(0, 3)
    |> then(&"#{seconds}#{&1}")
  end

  @base36 "0123456789abcdefghijklmnopqrstuvwxyz"
  defp maybe_add_id(data, true, seconds, microseconds) do
    seconds = String.to_integer(seconds)
    microseconds = String.to_integer(microseconds)

    base36_timestamp = to_base36(seconds * 10 ** 6 + microseconds)
    random_part = random_base36(22)

    id = "#{base36_timestamp}#{random_part}"

    data |> Map.put(:id, id)
  end

  defp maybe_add_id(data, _, _, _), do: data

  defp to_base36(int),
    do:
      int
      |> Integer.digits(36)
      |> Enum.map(&(@base36 |> String.at(&1)))
      |> Enum.join()

  defp random_base36(length) do
    1..length
    |> Enum.map(fn _ ->
      @base36
      |> String.at(Enum.random(0..35))
    end)
    |> Enum.join()
  end

  defp queue_key(queue_name), do: "rsmq:#{queue_name}:Q"

  defp handle_result({:error, error}, _, _), do: {:error, error}
  defp handle_result({:ok, resp}, expected, _) when resp == expected, do: :ok
  defp handle_result(_, _, error_message), do: {:error, error_message}

  defp validate_params(opts) do
    %{opts: opts, errors: []}
    |> validate_opt(:queue_name, &is_binary/1, "must be string")
    |> validate_opt(:vt, &is_integer/1, "must be integer")
    |> validate_opt(:delay, &is_integer/1, "must be integer")
    |> validate_opt(:maxsize, &is_integer/1, "must be integer")
    |> case do
      %{errors: []} -> :ok
      %{errors: errors} -> {:error, %{message: :invalid_params, errors: errors}}
    end
  end

  defp validate_opt(%{opts: opts} = validator, key, function, message) do
    if !Keyword.has_key?(opts, key) || function.(opts[key]) do
      validator
    else
      validator
      |> Map.update!(:errors, &(&1 ++ [{key, message}]))
    end
  end
end
