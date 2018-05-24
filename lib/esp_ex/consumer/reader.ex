defmodule EspEx.Consumer.Reader do
  @moduledoc false

  alias EspEx.Logger
  alias EspEx.Consumer
  alias EspEx.StreamName
  alias EspEx.RawEvent

  defstruct [
    :event_bus,
    :event_transformer,
    :stream_name,
    :identifier,
    :handler,
    :listen_opts
  ]

  def fetch_events(%__MODULE__{} = reader, pid, %{events: []} = state) do
    %{
      event_bus: event_bus,
      identifier: identifier,
      stream_name: stream_name
    } = reader

    events = read_batch(event_bus, identifier, stream_name, state)
    state = request_event_processing(pid, events, state)

    {:noreply, state}
  end

  def fetch_events(_reader, _pid, state), do: {:noreply, state}

  def consume_event(_reader, pid, %{events: []} = state) do
    GenServer.cast(pid, {:request_events})
    {:noreply, state}
  end

  def consume_event(
        %__MODULE__{} = reader,
        pid,
        %{
          events: [raw_event | events],
          meta: meta
        } = state
      ) do
    debug(fn ->
      "Consuming event #{raw_event.type}/#{raw_event.global_position}"
    end)

    %{handler: handler, event_transformer: event_transformer} = reader

    handle_event(handler, event_transformer, raw_event, meta)
    position = RawEvent.next_position(raw_event.position)
    global_position = raw_event.global_position
    global_position = RawEvent.next_global_position(global_position)

    state =
      state
      |> Map.put(:events, events)
      |> Map.put(:position, position)
      |> Map.put(:global_position, global_position)

    GenServer.cast(pid, {:process_event})

    {:noreply, state}
  end

  def listen(%__MODULE__{} = reader) do
    %{
      event_bus: event_bus,
      stream_name: stream_name,
      listen_opts: listen_opts
    } = reader

    event_bus.listen(stream_name, listen_opts)
  end

  def unlisten(%__MODULE__{} = reader, state) do
    %{
      event_bus: event_bus,
      listen_opts: listen_opts
    } = reader

    event_bus.unlisten(state.listener, listen_opts)
  end

  def debug(identifier, msg_or_fn) when is_function(msg_or_fn) do
    Logger.debug(fn -> "[##{identifier}] " <> msg.() end)
  end

  def debug(identifier, msg_or_fn) when is_bitstring(msg_or_fn) do
    Logger.debug(fn -> "[##{identifier}] " <> msg end)
  end

  defp request_event_processing(_, [], state), do: state

  defp request_event_processing(pid, events, state) do
    GenServer.cast(pid, {:process_event})
    Map.put(state, :events, events)
  end

  defp local_or_global_position(stream_name, %{
         position: pos,
         global_position: global_pos
       }) do
    case StreamName.category?(stream_name) do
      true -> {:global, global_pos}
      _ -> {:local, pos}
    end
  end

  defp read_batch(event_bus, identifier, stream_name, state) do
    {_, position} = local_or_global_position(stream_name, state)

    debug_position(identifier, stream_name, state)

    event_bus.read_batch(stream_name, position)
  end

  defp debug_position(identifier, stream_name, state) do
    {pos_type, pos} = local_or_global_position(stream_name, state)

    debug(identifier, fn -> "Requesting events from #{pos_type} #{pos}" end)
  end

  defp handle_event(handler, event_transformer, raw_event, meta) do
    event = event_transformer.to_event(raw_event)

    handler.handle(event, raw_event, meta)
  end
end
