defmodule EspEx.MessageStore.Postgres do
  @moduledoc """
  This is the real implementation of MessageStore. It will execute the needed
  queries on Postgres through Postgrex by calling the functions provided in
  [ESP](https://github.com/Carburetor/ESP/tree/master/app/config/functions/stream). You should be able to infer what to write, it's just passing the
  required arguments to the SQL functions and converting any returned value.
  Whenever a stream name is expected, please use the %StreamName struct and
  make sure to convert it to string.
  """

  use EspEx.MessageStore

  import EspEx.MessageStore,
    only: [
      is_version: 1,
      is_expected_version: 1,
      is_batch_size: 1
    ]

  alias EspEx.StreamName
  alias EspEx.RawEvent
  alias EspEx.RawEvent.Metadata

  @wrong_version "Wrong expected version:"
  @wrong_list "No messages"

  @read_batch_sql """
  select * from stream_read_batch(
    _stream_name := $1,
    _position    := $2,
    _batch_size  := $3
  )
  """
  @read_last_sql "select * from stream_read_last(_stream_name := $1)"
  @write_sql """
  select * from stream_write(
    _id               := $1::uuid,
    _stream_name      := $2,
    _type             := $3,
    _data             := $4,
    _metadata         := $5,
    _expected_version := $6
  )
  """
  @write_batch_sql """
  select * from stream_write_batch($1::minimal_message[], $2, $3)
  """
  @version_sql "select * from stream_version(_stream_name := $1)"
  @pg_notify_sql "select pg_notify($1, $2)"

  @impl EspEx.MessageStore
  @doc """
  Write has an optional expected_version argument. This argument could be one of:
  - nil: no version expected
  - no_stream: no message ever written to this stream, the Postgres
    stream_version position will return null (max(position) is null if no rows
    are present)
  - An integer (0+): Representing the expected version
  """
  def write!(%RawEvent{} = raw_event, expected_version \\ nil)
      when is_expected_version(expected_version) do
    expected_version = to_number_version(expected_version)
    params = raw_event_to_params(raw_event)
    params = params ++ [expected_version]

    query(@write_sql, params).rows
    |> rows_to_single_result
  rescue
    error in Postgrex.Error -> as_known_error!(error)
  end

  @impl EspEx.MessageStore
  @doc """
  - `raw_events` list of events to write
  - `stream_name` stream where events will be written to (will overwrite
    any stream_name provided in the raw_events)
  - optional `expected_version` argument. This argument could be one of:
    - `nil`: no version expected
    - `:no_stream`: no message ever written to this stream, the Postgres
      stream_version position will return null (max(position) is null if no
      rows are present)
    - An integer (0+): Representing the expected version
  """
  def write_batch!(
        raw_events,
        %StreamName{} = stream_name,
        expected_version \\ nil
      )
      when is_list(raw_events) and is_expected_version(expected_version) do
    raw_events_params = raw_events_to_params(raw_events)
    stream_name = to_string(stream_name)
    expected_version = to_number_version(expected_version)
    params = [raw_events_params, stream_name, expected_version]

    query(@write_batch_sql, params).rows
    |> rows_to_single_result
  rescue
    error in Postgrex.Error -> as_known_error!(error)
  end

  @impl EspEx.MessageStore
  @doc """
  Retrieve's the last stream by the stream_name (based on greatest position).
  """
  def read_last(%StreamName{} = stream_name) do
    query(@read_last_sql, [to_string(stream_name)]).rows
    |> rows_to_raw_events
    |> List.last()
  end

  @impl EspEx.MessageStore
  @doc """
  Retrieve steams by the stream_name, in batches of 10 by default.
  """
  def read_batch(%StreamName{} = stream_name, position \\ 0, batch_size \\ 10)
      when is_version(position) and is_batch_size(batch_size) do
    query(@read_batch_sql, [to_string(stream_name), position, batch_size]).rows
    |> rows_to_raw_events
  end

  @impl EspEx.MessageStore
  @doc """
  Retrieves the last message position, or nil if none are present
  """
  def read_version(%StreamName{} = stream_name) do
    query(@version_sql, [to_string(stream_name)]).rows
    |> rows_to_single_result
  end

  @impl EspEx.MessageStore
  @doc """
  Receives notifications as GenServer casts. Two types of notifications are
  received:

  - `{:notification, connection_pid, ref, channel, payload}` with a notify
    from Postgres (check
    [Postgrex documentation](https://hexdocs.pm/postgrex/Postgrex.Notifications.html#listen/3))
  - `{:reminder}` which is received every X seconds
  """
  def listen(%StreamName{} = stream_name, opts \\ []) do
    EspEx.MessageStore.Postgres.Notifications.listen(stream_name, opts)
  end

  @impl EspEx.MessageStore
  @doc """
  Stops notifications
  """
  def unlisten(ref, opts \\ []) do
    EspEx.MessageStore.Postgres.Notifications.unlisten(ref, opts)
  end

  @doc """
  Sends an SQL NOTIFY through postgres
  """
  @spec notify(channel :: String.t(), data :: String.t()) :: :ok
  def notify(channel, data) do
    query(@pg_notify_sql, [channel, data])
    :ok
  end

  defp to_number_version(:no_stream), do: -1
  defp to_number_version(nil), do: nil
  defp to_number_version(expected_version), do: expected_version

  defp query(raw_sql, parameters) do
    EspEx.MessageStore.Postgres.Repo
    |> Ecto.Adapters.SQL.query!(raw_sql, parameters)
  end

  defp raw_events_to_params(raw_events) do
    Enum.map(raw_events, &raw_event_to_minimal/1)
  end

  defp raw_event_to_minimal(%RawEvent{
         id: id,
         type: type,
         data: data,
         metadata: metadata
       }) do
    id = uuid_as_uuid(id)

    {id, type, data, metadata}
  end

  defp raw_event_to_params(%RawEvent{
         id: id,
         stream_name: stream_name,
         type: type,
         data: data,
         metadata: metadata
       }) do
    id = uuid_as_uuid(id)

    [id, to_string(stream_name), type, data, metadata]
  end

  defp rows_to_single_result([[value]]), do: value

  defp rows_to_raw_events(rows) do
    rows
    |> Enum.map(&row_to_raw_event/1)
  end

  defp row_to_raw_event([
         id,
         stream_name,
         type,
         position,
         global_position,
         data,
         metadata,
         time
       ]) do
    id = uuid_as_string(id)

    %RawEvent{
      id: id,
      stream_name: StreamName.from_string(stream_name),
      type: type,
      position: position,
      global_position: global_position,
      data: symbolize(data),
      metadata: struct(Metadata, metadata),
      time: time
    }
  end

  defp symbolize(map) do
    map
    |> Map.new(fn {k, v} -> {String.to_existing_atom(k), v} end)
  end

  defp as_known_error!(error) do
    message = to_string(error.postgres.message)

    cond do
      String.starts_with?(message, @wrong_version) ->
        raise EspEx.MessageStore.ExpectedVersionError, message: message

      String.starts_with?(message, @wrong_list) ->
        raise EspEx.MessageStore.EmptyBatchError, message: message

      true ->
        raise error
    end
  end

  defp uuid_as_uuid(id) do
    {:ok, uuid} =
      id
      |> Ecto.UUID.cast!()
      |> Ecto.UUID.dump()

    uuid
  end

  defp uuid_as_string(id) do
    Ecto.UUID.cast!(id)
  end
end
