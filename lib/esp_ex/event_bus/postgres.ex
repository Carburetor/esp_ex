defmodule EspEx.EventBus.Postgres do
  @moduledoc """
  This is the real implementation of EventBus. It will execute the needed
  queries on Postgres through Postgrex by calling the functions provided in
  [ESP](https://github.com/Carburetor/ESP/tree/master/app/config/functions/stream). You should be able to infer what to write, it's just passing the
  required arguments to the SQL functions and converting any returned value.
  Whenever a stream name is expected, please use the %StreamName struct and
  make sure to convert it to string.
  """

  # use EspEx.EventBus

  @doc """
  Write has an expected_version argument. This argument could be one of:
  - None: no version expected
  - NoStream: no message ever written to this stream, the Postgres
    stream_version position will return null (max(position) is null if no rows
    are present)
  - A number (0+): Representing the expected version
  """
  def write(args_here) do
  end

  @doc """
  listen will start listening for a specific stream name using Postgres LISTEN.
  Check Postgrex to see how to use Postgres LISTEN
  """
  #def listen(args_here) do
  #end

  @doc """
  unlisten stops Postgres LISTEN
  """
  def unlisten(args_here) do
  end

  def listen(channel) do
    {:ok, pid} = Postgrex.Notifications.start_link(database_config)
    IO.inspect pid, label: "PID" 
    {:ok, ref} = Postgrex.Notifications.listen(pid, channel)
    IO.inspect ref, label: "REF" 
    :ok
  end

  def notify(channel, data) do
    # NOTIFY dave, 'dave'
    sql = "select pg_notify($1, $2)"
    IO.inspect sql, label: "WTF"
    IO.inspect query(sql, [channel, data]), label: "NOTIFY QUERY"
  end

  def write(id, stream_name, type, data, opts \\ []) do
    sql = """
     select * from stream_write_message(
       _id               := $1,
       _stream_name      := $2,
       _type             := $3,
       _data             := $4,
       _metadata         := $5,
       _expected_version := $6
    )
    """

    query(sql, [
      id,
      stream_name,
      type,
      data,
      opts[:metadata] || nil,
      opts[:expected_version] || nil
    ])
  end

  def get_batch(name, opts \\ []) do
    sql = """
    select * from stream_get_batch(
    _stream_name := $1, _position := $2, _batch_size  := $3 
    )
    """

    {ok, result} = query(sql, [name, opts[:position] || 0, opts[:batch_size] || 10])
    result.rows
      |> rows_to_streams
  end

  defp query(raw_sql, parameters) do
    Ecto.Adapters.SQL.query(EspEx.Repo, raw_sql, parameters)
  end

  # TODO are we calling these messages? streams? events?
  defp rows_to_streams(rows) do
    for row <- rows do
      [id, stream_name, type, position, global_position, data, metadata, time] = row
      %{
        id: id,
        stream_name: stream_name,
        type: type,
        position: position,
        global_position: global_position,
        data: data,
        metadata: metadata,
        time: time
      }
    end
  end

  def database_config do
    pg_config = Application.get_env(:esp_ex, EspEx.Repo)
  end
end
