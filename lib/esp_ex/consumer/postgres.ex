defmodule EspEx.Consumer.Postgres do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events using postgres
  adapter
  """

  alias EspEx.Consumer
  alias EspEx.Consumer.Config
  alias EspEx.Consumer.State

  @doc """
  - `:event_transformer` **required** an `EspEx.EventTransformer`
    implementation
  - `:stream_name` **required** a `EspEx.StreamName`
  - `:identifier` (optional) a `String` identifying uniquely this consumer.
    Defaults to the current module name
  - `:handler` (optional) a `EspEx.Handler` implementation. Defaults to using
    the current module
  - `:listen_opts` (optional) options that will be provided to the `message_store`
    that listen call as last argument
  """
  defmacro __using__(opts \\ []) do
    identifier = Keyword.get(opts, :identifier, __CALLER__.module)
    identifier = Consumer.identifier(identifier)

    opts =
      opts
      |> Keyword.put(:message_store, EspEx.MessageStore.Postgres)
      |> Keyword.put_new(:identifier, identifier)
      |> Keyword.put_new(:handler, __CALLER__.module)

    quote location: :keep, bind_quoted: [opts: opts, identifier: identifier] do
      @ident identifier
      @conf Config.new(opts)

      use GenServer

      @impl GenServer
      def init(meta) do
        {:ok, listener} = Consumer.listen(@conf)

        state = %State{meta: meta, listener: listener}
        GenServer.cast(self(), {:request_events})

        {:ok, state}
      end

      @impl GenServer
      def handle_cast({:request_events}, %State{} = state) do
        Consumer.fetch_events(@conf, self(), state)
      end

      @impl GenServer
      def handle_cast({:process_event}, %State{} = state) do
        Consumer.consume_event(@conf, self(), state)
      end

      @impl GenServer
      def handle_info(
            {:notification, _, _, channel, _payload},
            %State{} = state
          ) do
        Consumer.debug(@ident, fn -> "Notification for stream: #{channel}" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, state}
      end

      @impl GenServer
      def handle_info({:reminder}, %State{} = state) do
        Consumer.debug(@ident, fn -> "Reminder" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, state}
      end

      @impl GenServer
      def terminate(:normal, state), do: Consumer.unlisten(@conf, state)
      def terminate(:shutdown, state), do: Consumer.unlisten(@conf, state)
      def terminate({:shutdown, _}, state), do: Consumer.unlisten(@conf, state)
      defoverridable terminate: 2
    end
  end
end
