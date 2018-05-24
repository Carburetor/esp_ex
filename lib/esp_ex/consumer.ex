defmodule EspEx.Consumer do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events. You might want to
  use `EspEx.Consumer.Postgres` specialization which internally uses
  `EspEx.EventBus.Postgres.listen`
  """

  alias EspEx.Logger
  alias EspEx.Consumer.Reader

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

    reader = %__MODULE__.Reader{
      event_bus: event_bus,
      event_transformer: event_transformer,
      stream_name: stream_name,
      identifier: identifier,
      handler: handler,
      listen_opts: listen_opts
    }

    quote location: :keep, bind_quoted: [reader: reader] do
      use GenServer

      @impl GenServer
      def init(meta) do
        {:ok, listener} = Reader.listen(reader)

        state = %consumer{meta: meta, listener: listener}
        GenServer.cast(self(), {:request_events})

        {:ok, state}
      end

      @impl GenServer
      def handle_cast({:request_events}, %consumer{} = state) do
        Reader.fetch_events(reader, self(), state)
      end

      @impl GenServer
      def handle_cast({:process_event}, %consumer{} = state) do
        Reader.consume_event(reader, self(), state)
      end

      @impl GenServer
      def terminate(:normal, state), do: Reader.unlisten(reader, state)
      def terminate(:shutdown, state), do: Reader.unlisten(reader, state)
      def terminate({:shutdown, _}, state), do: Reader.unlisten(reader, state)
      defoverridable terminate: 2
    end
  end
end
