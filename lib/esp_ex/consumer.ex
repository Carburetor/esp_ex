defmodule EspEx.Consumer do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events. You might want to
  use `EspEx.Consumer.Postgres` specialization which internally uses
  `EspEx.EventBus.Postgres.listen`
  """

  alias EspEx.Logger

  defstruct listener: nil,
            position: 0,
            global_position: 0,
            events: [],
            meta: nil

  @spec identifier(consumer :: module()) :: String.t()
  @doc """
  Determines an identifier for the given module as a string
  """
  def identifier(consumer) when is_atom(consumer) do
    to_string(consumer)
  end

  @doc false
  defmacro local_or_global_position(stream_name, state) do
    quote location: :keep,
          bind_quoted: [stream_name: stream_name, state: state] do
      %{position: pos, global_position: global_pos} = state

      case EspEx.StreamName.category?(stream_name) do
        true -> {:global, global_pos}
        _ -> {:local, pos}
      end
    end
  end

  @doc false
  defmacro read_batch(event_bus, identifier, stream_name, state) do
    quote location: :keep,
          bind_quoted: [
            identifier: identifier,
            stream_name: stream_name,
            state: state,
            module: __MODULE__
          ] do
      {_, position} = module.local_or_global_position(stream_name, state)

      module.debug_position(identifier, stream_name, state)

      event_bus.read_batch(stream_name, position)
    end
  end

  @doc false
  defmacro debug_position(identifier, stream_name, state) do
    quote location: :keep,
          bind_quoted: [
            identifier: identifier,
            stream_name: stream_name,
            state: state,
            module: __MODULE__
          ] do
      {pos_type, pos} = module.local_or_global_position(stream_name, state)

      module.debug(identifier, fn ->
        "Requesting events from #{pos_type} #{pos}"
      end)
    end
  end

  @doc false
  defmacro debug(identifier, msg_or_fn) when is_function(msg_or_fn) do
    quote location: :keep,
          bind_quoted: [identifier: identifier, msg_or_fn: msg_or_fn] do
      Logger.debug(fn -> "[##{identifier}] " <> msg.() end)
    end
  end

  @doc false
  defmacro debug(identifier, msg_or_fn) when is_bitstring(msg_or_fn) do
    quote location: :keep,
          bind_quoted: [identifier: identifier, msg_or_fn: msg_or_fn] do
      Logger.debug(fn -> "[##{identifier}] " <> msg end)
    end
  end

  @doc false
  defmacro handle_event(handler, event_transformer, raw_event, meta) do
    quote location: :keep,
          bind_quoted: [
            handler: handler,
            event_transformer: event_transformer,
            raw_event: raw_event,
            meta: meta
          ] do
      event = event_transformer.to_event(raw_event)

      handler.handle(event, raw_event, meta)
    end
  end

  @doc """
  - `:event_bus` **required** an `EspEx.EventBus` implementation
  - `:event_transformer` **required** an `EspEx.EventTransformer`
    implementation
  - `:stream_name` **required** a `EspEx.StreamName`
  - `:identifier` (optional) a `String` identifying uniquely this consumer.
    Defaults to the current module name
  - `:handler` (optional) a `EspEx.Handler` implementation. Defaults to using
    the current module
  - `:listen_opts` (optional) options that will be provided to to the
    `event_bus` that listen call as last argument
  """
  defmacro __using__(opts \\ []) do
    event_bus = Keyword.get(opts, :event_bus)
    event_transformer = Keyword.get(opts, :event_transformer)
    stream_name = Keyword.get(opts, :stream_name)
    default_identifier = __MODULE__.identifier(__CALLER__.module)
    identifier = Keyword.get(opts, :identifier, default_identifier)
    handler = Keyword.get(opts, :handler, __CALLER__.module)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    consumer = __MODULE__

    quote location: :keep,
          bind_quoted: [
            :event_bus,
            :event_transformer,
            :stream_name,
            :identifier,
            :handler,
            :listen_opts,
            :consumer
          ] do
      use GenServer

      @impl GenServer
      def init(meta) do
        {:ok, listener} = event_bus.listen(stream_name, listen_opts)

        state = %consumer{meta: meta, listener: listener}
        GenServer.cast(self(), {:request_events})

        {:ok, state}
      end

      @impl GenServer
      def handle_cast({:request_events}, %consumer{} = state) do
        fetch_events(state)
      end

      @impl GenServer
      def handle_cast({:process_event}, %consumer{} = state) do
        consume_event(state)
      end

      @impl GenServer
      def terminate(:normal, state),
        do: event_bus.unlisten(state.listener, listen_opts)

      def terminate(:shutdown, state),
        do: event_bus.unlisten(state.listener, listen_opts)

      def terminate({:shutdown, _}, state),
        do: event_bus.unlisten(state.listener, listen_opts)

      defoverridable terminate: 2

      defp fetch_events(%{events: []} = state) do
        events = read_batch(state)

        state =
          case events do
            [] ->
              state

            _ ->
              GenServer.cast(self(), {:process_event})
              Map.put(state, :events, events)
          end

        {:noreply, state}
      end

      defp fetch_events(state), do: {:noreply, state}

      defp consume_event(%{events: []} = state) do
        GenServer.cast(self(), {:request_events})
        {:noreply, state}
      end

      defp consume_event(
             %{
               events: [raw_event | events],
               meta: meta
             } = state
           ) do
        consumer.debug(fn ->
          "[##{identifier}] Consuming event " <>
            "#{raw_event.type}/#{raw_event.position}"
        end)

        consumer.handle_event(handler, event_transformer, raw_event, meta)
        position = EspEx.RawEvent.next_position(raw_event.position)
        global_position = raw_event.global_position
        global_position = EspEx.RawEvent.next_global_position(global_position)

        state =
          state
          |> Map.put(:events, events)
          |> Map.put(:position, position)
          |> Map.put(:global_position, global_position)

        GenServer.cast(self(), {:process_event})

        {:noreply, state}
      end

      defp read_batch(%{position: pos, global_position: global_pos}) do
        {pos_type, position} =
          consumer.local_or_global_position(
            stream_name,
            pos,
            global_pos
          )

        Logger.debug(fn ->
          "[##{identifier}] Requesting events P#{pos} G#{global_pos}, " <>
            "used #{pos_type}"
        end)

        event_bus.read_batch(stream_name, position)
      end
    end
  end
end
