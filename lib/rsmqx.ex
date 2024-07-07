defmodule Rsmqx do
  @moduledoc """
  Documentation for `Rsmqx`.
  """

  @external_resource "priv/redis/receive_message.lua"

  @queue_list "rsmq:QUEUES"
  @receive_message_src File.read!("priv/redis/receive_message.lua")

  @doc """
  Create a queue for messages
  """
  def create_queue(conn, queue_name, params \\ []) do
    key = queue_data_key(queue_name)

    with :ok <- validate_params([{:queue_name, queue_name} | params]),
         :ok <- do_create_queue(conn, key, params),
         :ok <- index_queue(conn, queue_name) do
      :ok
    else
      error -> error
    end
  end

  defp do_create_queue(conn, key, params) do
    pipeline =
      [
        ["hsetnx", key, :vt, params[:vt] || 30],
        ["hsetnx", key, :delay, params[:delay] || 0],
        ["hsetnx", key, :maxsize, params[:maxsize] || 65536]
      ]

    Redix.transaction_pipeline(conn, pipeline)
    |> handle_result([1, 1, 1], :queue_exists)
  end

  defp index_queue(conn, queue_name) do
    Redix.command(conn, ["sadd", @queue_list, queue_name])
    |> handle_result(1, :queue_index_exists)
  end

  @doc """
  List all queues
  """
  def list_queues(conn), do: Redix.command!(conn, ["smembers", @queue_list])

  @doc """
  Delete queue
  """
  def delete_queue(conn, queue_name) do
    data_key = queue_data_key(queue_name)
    messages_key = queue_messages_key(queue_name)

    Redix.pipeline(
      conn,
      [
        ["del", data_key, messages_key],
        ["srem", @queue_list, queue_name]
      ]
    )
    |> case do
      {:ok, [0, _]} -> {:error, :queue_not_found}
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Insert message into queue
  """
  def send_message(conn, queue_name, message, params \\ []) do
    params = [queue_name: queue_name, message: message] ++ params

    with :ok <- validate_params(params),
         {:ok, queue} <- get_queue_attrs(conn, queue_name, true),
         :ok <- validate_message_size(queue, message) do
      messages_key = queue_messages_key(queue_name)
      data_key = queue_data_key(queue_name)
      delay = params[:delay] || queue.delay

      Redix.transaction_pipeline(
        conn,
        [
          ["zadd", messages_key, queue.ts + delay * 1000, queue.id],
          ["hset", data_key, queue.id, message],
          ["hincrby", data_key, "totalsent", 1]
        ]
      )
      |> case do
        {:ok, [1, 1, _]} -> {:ok, queue.id}
        {:ok, result} -> {:error, %{message: :unexpected_redis_return, result: result}}
        error -> error
      end
    else
      error -> error
    end
  end

  defp validate_message_size(queue, message) do
    if queue.maxsize == -1 || String.length(message) <= queue.maxsize,
      do: :ok,
      else: {:error, :message_too_long}
  end

  @doc """
  Receive the next message in the queue
  """
  def receive_message(conn, queue_name, params \\ []) do
    with :ok <- validate_params(params),
         {:ok, queue} <- get_queue_attrs(conn, queue_name),
         queue <- Map.put(queue, :vt, params[:vt] || queue.vt),
         any <- do_receive_message(conn, queue_name, queue) do
      any
    else
      error -> error
    end
  end

  defp do_receive_message(conn, queue_name, queue, retrial? \\ false) do
    hash = get_receive_script_hash(conn)
    key = queue_messages_key(queue_name)

    Redix.command(conn, ["evalsha", hash, 3, key, queue.ts, queue.ts + queue.vt * 1000])
    |> handle_receive_result(conn, queue_name, queue, retrial?)
  end

  defp get_receive_script_hash(conn) do
    case :persistent_term.get(:rsmqx_receive_message_script, nil) do
      nil -> load_receive_script(conn)
      hash -> hash
    end
  end

  # This is to try to reload the script in case of flush or restart,
  # but prevent infinite reload tries loop in case of being unable to load script
  defp handle_receive_result(result, conn, queue_name, queue, retrial?) do
    case result do
      {:error,
       %Redix.Error{
         message: "NOSCRIPT No matching script. Please use EVAL."
       }} ->
        if retrial? do
          raise "unable to load \"receive_message.lua\" script"
        else
          load_receive_script(conn)
          do_receive_message(conn, queue_name, queue, true)
        end

      result ->
        result
    end
  end

  defp load_receive_script(conn) do
    with {:ok, hash} <- Redix.command(conn, ["script", "load", @receive_message_src]) do
      :persistent_term.put(:rsmqx_receive_message_script, hash)

      hash
    else
      _ -> raise "Error on loading lua script for receiving messages"
    end
  end

  @doc """
  Remove message from queue
  """
  def delete_message(conn, queue_name, id) do
    messages_key = queue_messages_key(queue_name)
    data_key = queue_data_key(queue_name)

    Redix.pipeline(
      conn,
      [
        ["zrem", messages_key, id],
        ["hdel", data_key, id, "#{id}:rc", "#{id}:fr"]
      ]
    )
    |> case do
      {:ok, [1, hdel]} when hdel > 0 -> :ok
      {:ok, _} -> {:error, :message_not_found}
      error -> error
    end
  end

  defp get_queue_attrs(conn, queue_name, create_id? \\ false) do
    key = queue_data_key(queue_name)

    Redix.transaction_pipeline(
      conn,
      [
        ["hmget", key, "vt", "delay", "maxsize"],
        ["time"]
      ]
    )
    |> case do
      {:ok, [[nil, _, _], _]} ->
        {:error, :queue_not_found}

      {:ok, [[vt, delay, maxsize], [seconds, microseconds]]} ->
        timestamp = create_timestamp(seconds, microseconds)

        %{
          vt: String.to_integer(vt),
          delay: String.to_integer(delay),
          maxsize: String.to_integer(maxsize),
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
    |> String.to_integer()
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

  defp queue_data_key(queue_name), do: "rsmq:#{queue_name}:Q"
  defp queue_messages_key(queue_name), do: "rsmq:#{queue_name}"

  defp handle_result({:error, error}, _, _), do: {:error, error}
  defp handle_result({:ok, resp}, expected, _) when resp == expected, do: :ok
  defp handle_result(_, _, error_message), do: {:error, error_message}

  defp validate_params(params) do
    %{params: params, errors: []}
    |> validate_param(:queue_name, &is_binary/1, "must be string")
    |> validate_param(:message, &is_binary/1, "must be string")
    |> validate_param(:vt, &is_integer/1, "must be integer")
    |> validate_param(:delay, &is_integer/1, "must be integer")
    |> validate_param(:maxsize, &is_integer/1, "must be integer")
    |> case do
      %{errors: []} -> :ok
      %{errors: errors} -> {:error, %{message: :invalid_params, errors: errors}}
    end
  end

  defp validate_param(%{params: params} = validator, key, function, message) do
    if !Keyword.has_key?(params, key) || function.(params[key]) do
      validator
    else
      validator
      |> Map.update!(:errors, &(&1 ++ [{key, message}]))
    end
  end
end
