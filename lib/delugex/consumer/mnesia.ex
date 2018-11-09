defmodule Delugex.Consumer.Mnesia do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events using postgres
  adapter.
  The value passed to this Consumer as second argument of `start_link` is
  passed as 3rd argument of `handle`
  """

  alias Delugex.Consumer
  alias Delugex.Consumer.Config
  alias Delugex.Consumer.State

  @doc """
  - `:event_transformer` **required** an `Delugex.EventTransformer`
    implementation
  - `:stream_name` **required** a `Delugex.StreamName`
  - `:identifier` (optional) a `String` identifying uniquely this consumer.
    Defaults to the current module name
  - `:handler` (optional) a `Delugex.Handler` implementation. Defaults to using
    the current module
  - `:listen_opts` (optional) options that will be provided to the `message_store`
    that listen call as last argument
  """
  defmacro __using__(opts \\ []) do
    identifier = Keyword.get(opts, :identifier, __CALLER__.module)
    identifier = Consumer.identifier(identifier)

    opts =
      opts
      |> Keyword.put(:message_store, Delugex.MessageStore.Mnesia)
      |> Keyword.put_new(:identifier, identifier)
      |> Keyword.put_new(:handler, __CALLER__.module)

    quote location: :keep, bind_quoted: [opts: opts, identifier: identifier] do
      @ident identifier
      @conf Config.new(opts)

      use GenServer

      def start_link(state \\ nil, opts \\ []) do
        GenServer.start_link(__MODULE__, state, opts)
      end

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
            {:mnesia_table_event, {:write, event, _}},
            %State{} = state
          ) do
        channel = elem(event, 5)

        listener_stream =
          state
          |> Map.fetch!(:listener)
          |> Map.fetch!(:stream_name)

        if Delugex.StreamName.subset?(channel, listener_stream) do
          Consumer.debug(@ident, fn ->
            "Notification for stream: #{channel}"
          end)

          GenServer.cast(self(), {:request_events})
        end

        {:noreply, state}
      end

      @impl GenServer
      def handle_info(
            {:mnesia_table_event, _},
            %State{} = state
          ) do
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
      def terminate(_, state), do: Consumer.unlisten(@conf, state)
      defoverridable terminate: 2
    end
  end
end
