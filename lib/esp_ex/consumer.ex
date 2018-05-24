defmodule EspEx.Consumer do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events. You might want to
  use `EspEx.Consumer.Postgres` specialization which internally uses
  `EspEx.MessageStore.Postgres.listen`
  """

  alias EspEx.Consumer.Config
  alias EspEx.Consumer.State
  alias EspEx.Logger
  alias EspEx.StreamName
  alias EspEx.RawEvent

  @doc """
  Determines an identifier for the given module as a string
  """
  @spec identifier(consumer :: module()) :: String.t()
  def identifier(consumer) when is_atom(consumer) do
    to_string(consumer)
  end

  @spec identifier(consumer :: String.t()) :: String.t()
  def identifier(consumer) when is_bitstring(consumer) do
    consumer
  end

  @doc """
  Fetch new events and refill the state with those
  """
  @spec fetch_events(
          config :: EspEx.Consumer.Config.t(),
          pid :: GenServer.server(),
          state :: EspEx.Consumer.State.t()
        ) :: {:noreply, EspEx.Consumer.State.t()}
  def fetch_events(%Config{} = config, pid, %{events: []} = state) do
    %{
      message_store: message_store,
      identifier: identifier,
      stream_name: stream_name
    } = config

    events = read_batch(message_store, identifier, stream_name, state)
    state = request_event_processing(pid, events, state)

    {:noreply, state}
  end

  def fetch_events(_config, _pid, state), do: {:noreply, state}

  def consume_event(_config, pid, %{events: []} = state) do
    GenServer.cast(pid, {:request_events})
    {:noreply, state}
  end

  @doc """
  Removes an event from the state and passes it to a handler
  """
  @spec consume_event(
          config :: EspEx.Consumer.Config.t(),
          pid :: GenServer.server(),
          state :: EspEx.Consumer.State.t()
        ) :: {:noreply, EspEx.Consumer.State.t()}
  def consume_event(
        %Config{} = config,
        pid,
        %{
          events: [raw_event | events],
          meta: meta
        } = state
      ) do
    debug(config.identifier, fn ->
      "Consuming event #{raw_event.type}/#{raw_event.global_position}"
    end)

    %{handler: handler, event_transformer: event_transformer} = config

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

  @doc """
  Start listening to incoming events
  """
  @spec listen(config :: EspEx.Consumer.Config.t()) ::
          {:ok, EspEx.MessageStore.listen_ref()} | {:error, any}
  def listen(%Config{} = config) do
    %{
      message_store: message_store,
      stream_name: stream_name,
      listen_opts: listen_opts
    } = config

    message_store.listen(stream_name, listen_opts)
  end

  @doc """
  Stop listening to events
  """
  @spec unlisten(
          config :: EspEx.Consumer.Config.t(),
          state :: EspEx.Consumer.State.t()
        ) :: {:ok, EspEx.MessageStore.listen_ref()} | {:error, any}
  def unlisten(%Config{} = config, %State{} = state) do
    %{
      message_store: message_store,
      listen_opts: listen_opts
    } = config

    message_store.unlisten(state.listener, listen_opts)
  end

  @doc """
  Writes a debug line for the given consumer, based on identifier
  """
  @spec debug(
          identifier :: String.t(),
          msg_or_fn :: String.t() | fun()
        ) :: no_return
  def debug(identifier, msg_or_fn) when is_function(msg_or_fn) do
    Logger.debug(fn -> "[##{identifier}] " <> msg_or_fn.() end)
  end

  def debug(identifier, msg_or_fn) when is_bitstring(msg_or_fn) do
    Logger.debug(fn -> "[##{identifier}] " <> msg_or_fn end)
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

  defp read_batch(message_store, identifier, stream_name, state) do
    {_, position} = local_or_global_position(stream_name, state)

    debug_position(identifier, stream_name, state)

    message_store.read_batch(stream_name, position)
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
