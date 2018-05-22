defmodule EspEx.Consumer do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events. You might want to
  use `EspEx.Consumer.Postgres` specialization which internally uses
  `EspEx.EventBus.Postgres.listen`
  """

  defstruct listener: nil,
            position: 0,
            global_position: 0,
            events: [],
            meta: nil

  def identifier(consumer_module) when is_atom(consumer_module) do
    to_string(consumer_module)
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
    identifier = Keyword.get(opts, :identifier, nil)
    handler = Keyword.get(opts, :handler, nil)
    listen_opts = Keyword.get(opts, :listen_opts, [])

    quote location: :keep do
      use GenServer

      @event_bus unquote(event_bus)
      @event_transformer unquote(event_transformer)
      @stream_name unquote(stream_name)
      @listen_opts unquote(listen_opts)
      @consumer unquote(__MODULE__)
      @identifier unquote(identifier) || @consumer.identifier(__MODULE__)

      defp handler do
        case unquote(handler) do
          nil -> __MODULE__
          _ -> unquote(handler)
        end
      end

      @impl GenServer
      def init(meta) do
        {:ok, listener} = @event_bus.listen(@stream_name, @listen_opts)

        consumer =
          %@consumer{meta: meta}
          |> Map.put(:listener, listener)

        GenServer.cast(self(), {:request_events})

        {:ok, consumer}
      end

      @impl GenServer
      def handle_info(
            {:notification, _, _, channel, _payload},
            %@consumer{} = consumer
          ) do
        debug(fn -> "Notification for stream: #{channel}" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, consumer}
      end

      @impl GenServer
      def handle_info({:reminder}, %@consumer{} = consumer) do
        debug(fn -> "Reminder" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, consumer}
      end

      @impl GenServer
      def handle_cast({:request_events}, %@consumer{} = consumer) do
        fetch_events(consumer)
      end

      @impl GenServer
      def handle_cast({:process_event}, %@consumer{} = consumer) do
        consume_event(consumer)
      end

      @impl GenServer
      def terminate(:normal, consumer), do: unlisten(consumer)
      def terminate(:shutdown, consumer), do: unlisten(consumer)
      def terminate({:shutdown, _}, consumer), do: unlisten(consumer)
      defoverridable terminate: 2

      defp fetch_events(%{events: []} = consumer) do
        events = read_batch(consumer)

        consumer =
          case events do
            [] ->
              consumer

            _ ->
              process_next_event()
              Map.put(consumer, :events, events)
          end

        {:noreply, consumer}
      end

      defp fetch_events(consumer), do: {:noreply, consumer}

      defp consume_event(%{events: []} = consumer) do
        GenServer.cast(self(), {:request_events})
        {:noreply, consumer}
      end

      defp consume_event(
             %{
               events: [raw_event | events],
               meta: meta
             } = consumer
           ) do
        debug(fn ->
          "Consuming event #{raw_event.type}/#{raw_event.position}"
        end)

        handle_event(raw_event, meta)
        position = EspEx.RawEvent.next_position(raw_event.position)
        global_position = raw_event.global_position
        global_position = EspEx.RawEvent.next_global_position(global_position)

        consumer =
          consumer
          |> Map.put(:events, events)
          |> Map.put(:position, position)
          |> Map.put(:global_position, global_position)

        process_next_event()

        {:noreply, consumer}
      end

      defp handle_event(raw_event, meta) do
        event = @event_transformer.to_event(raw_event)

        handler().handle(event, raw_event, meta)
      end

      defp read_batch(%{position: pos, global_position: global_pos}) do
        {pos_type, position} = local_or_global_position(pos, global_pos)

        debug(fn ->
          "Requesting events P#{pos} G#{global_pos}, used #{pos_type}"
        end)

        @event_bus.read_batch(@stream_name, position)
      end

      defp unlisten(%{listener: listener}) do
        @event_bus.unlisten(listener, @listen_opts)
      end

      defp process_next_event do
        GenServer.cast(self(), {:process_event})
      end

      defp local_or_global_position(pos, global_pos) do
        case EspEx.StreamName.category?(@stream_name) do
          true -> {:global, global_pos}
          _ -> {:local, pos}
        end
      end

      defp debug(msg) when is_function(msg) do
        EspEx.Logger.debug(fn -> "[##{@identifier}] " <> msg.() end)
      end

      defp debug(msg) do
        EspEx.Logger.debug(fn -> "[##{@identifier}] " <> msg end)
      end
    end
  end
end
