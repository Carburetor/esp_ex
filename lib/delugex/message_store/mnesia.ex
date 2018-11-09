defmodule Delugex.MessageStore.Mnesia do
  @moduledoc """
  This is the real implementation of MessageStore.
  You should be able to infer what to write, it's just passing the
  required arguments to the SQL functions and converting any returned value.
  Whenever a stream name is expected, please use the %StreamName struct and
  make sure to convert it to string.
  """

  use Delugex.MessageStore

  import Delugex.MessageStore,
    only: [
      is_version: 1,
      is_expected_version: 1,
      is_batch_size: 1
    ]

  alias Delugex.StreamName
  alias Delugex.Event
  alias Delugex.Event.Raw
  alias Delugex.Event.Metadata
  alias Delugex.MessageStore.Mnesia.Repo
  alias Delugex.MessageStore.Mnesia.ExpectedVersionError, as: MnesiaVersionError

  def start do
    Repo.create()
  end

  def stop do
    Repo.delete()
  end

  @impl Delugex.MessageStore
  @doc """
  Write has an optional expected_version argument. This argument could be one of:
  - nil: no version expected
  - no_stream: no message ever written to this stream, the Postgres
    stream_version position will return null (max(position) is null if no rows
    are present)
  - An integer (0+): Representing the expected version
  """
  def write!(%Event{} = event, expected_version \\ nil)
      when is_expected_version(expected_version) do
    expected_version = to_number_version(expected_version)
    params = encode_event(event)
    params = params ++ [expected_version]

    case Repo.write_message(params) do
      {:atomic, version} ->
        version

      {_, %MnesiaVersionError{} = error} ->
        as_known_error!(error)
    end
  end

  @impl Delugex.MessageStore
  @doc """
  - `events` list of events to write
  - `stream_name` stream where events will be written to (will overwrite
    any stream_name provided in the events)
  - optional `expected_version` argument. This argument could be one of:
    - `nil`: no version expected
    - `:no_stream`: no message ever written to this stream, the Postgres
      stream_version position will return null (max(position) is null if no
      rows are present)
    - An integer (0+): Representing the expected version
  """
  def write_batch!(
        events,
        stream_name,
        expected_version \\ nil
      )
      when is_list(events) and is_expected_version(expected_version) do
    insertables =
      events
      |> Stream.map(fn event -> Map.put(event, :stream_name, stream_name) end)
      |> Stream.with_index()
      |> Stream.map(fn {event, index} ->
        case index do
          0 -> encode_event(event) ++ [to_number_version(expected_version)]
          _ -> encode_event(event) ++ [nil]
        end
      end)

    {:atomic, version} = Repo.write_messages(insertables)
    version
  end

  @impl Delugex.MessageStore
  @doc """
  Retrieve's the last stream by the stream_name (based on greatest position).
  """
  def read_last(stream_name) do
    {:atomic, message} = Repo.get_last_message(stream_name)

    row_to_event_raw(message)
  end

  @impl Delugex.MessageStore
  @doc """
  Retrieve steams by the stream_name, in batches of 10 by default.
  """
  def read_batch(stream_name, position \\ 0, batch_size \\ 10)
      when is_version(position) and is_batch_size(batch_size) do
    query_fun =
      case StreamName.category?(stream_name) do
        true -> &Repo.get_category_messages/3
        false -> &Repo.get_stream_messages/3
      end

    {
      :atomic,
      {records, _cont}
    } = query_fun.(stream_name, position, batch_size)

    rows_to_events(records)
  end

  @impl Delugex.MessageStore
  @doc """
  Retrieves the last message position, or :no_stream if none are present
  """
  def read_version(stream_name) do
    {:atomic, version} = Repo.stream_version(stream_name)

    case version do
      nil -> :no_stream
      _ -> version
    end
  end

  @impl Delugex.MessageStore
  @doc """
  Receives notifications as GenServer casts. Two types of notifications are
  received:

  - `{:notification, connection_pid, ref, channel, payload}` with a notify
    from Postgres (check
    [Postgrex documentation](https://hexdocs.pm/postgrex/Postgrex.Notifications.html#listen/3))
  - `{:reminder}` which is received every X seconds
  """
  def listen(stream_name, opts \\ []) do
    stream_name = StreamName.to_string(stream_name)
    Repo.Notifications.listen(stream_name, opts)
  end

  @impl Delugex.MessageStore
  @doc """
  Stops notifications
  """
  def unlisten(ref, opts \\ []) do
    Repo.Notifications.unlisten(ref, opts)
  end

  defp to_number_version(:no_stream), do: -1
  defp to_number_version(nil), do: nil
  defp to_number_version(expected_version), do: expected_version

  defp encode_event(%Event{
         id: id,
         stream_name: stream_name,
         type: type,
         data: data,
         metadata: metadata
       }) do
    id = cast_uuid_as_string(id)
    stream_name = StreamName.to_string(stream_name)

    [id, stream_name, type, data, metadata]
  end

  defp rows_to_events(rows) do
    rows
    |> Enum.map(&row_to_event_raw/1)
  end

  defp row_to_event_raw([
         id,
         stream_name,
         type,
         position,
         global_position,
         data,
         metadata,
         time
       ]) do
    id = cast_uuid_as_string(id)

    %Raw{
      id: decode_id(id),
      stream_name: decode_stream_name(stream_name),
      type: type,
      position: position,
      global_position: global_position,
      data: decode_data(data),
      metadata: decode_metadata(metadata),
      time: time
    }
  end

  defp symbolize(map) do
    map
    |> Map.new(fn {k, v} -> {String.to_existing_atom(k), v} end)
  end

  defp as_known_error!(%MnesiaVersionError{} = error) do
    raise Delugex.MessageStore.ExpectedVersionError, message: error.message
  end

  defp as_known_error!(error) do
    raise error
  end

  defp cast_uuid_as_string(id) do
    Ecto.UUID.cast!(id)
  end

  defp decode_stream_name(text_stream_name) do
    decoder =
      __MODULE__
      |> Delugex.Config.get(:stream_name, [])
      |> Keyword.get(:decoder, Delugex.Stream.Name)

    decoder.decode(text_stream_name)
  end

  defp decode_metadata(map) do
    metadata =
      map
      |> decode_json()
      |> symbolize()

    struct(Metadata, metadata)
  end

  defp decode_data(map) do
    map
    |> decode_json()
    |> symbolize()
  end

  defp decode_id(id) do
    cast_uuid_as_string(id)
  end

  defp decode_json(text) do
    decoder =
      __MODULE__
      |> Delugex.Config.get(:json, [])
      |> Keyword.get(:decoder, Jason)

    decoder.decode!(text)
  end
end
