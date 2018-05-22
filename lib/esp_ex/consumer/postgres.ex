defmodule EspEx.Consumer.Postgres do
  @moduledoc """
  Listen to a stream allowing to handle any incoming events using postgres
  adapter
  """

  @doc """
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
    opts = Keyword.put(opts, :event_bus, EspEx.EventBus.Postgres)
    identifier = Keyword.get(opts, :identifier, __CALLER__.module)

    quote do
      use EspEx.Consumer, opts

      @module EspEx.Consumer
      @identifier unquote(identifier)

      @impl GenServer
      def handle_info(
            {:notification, _, _, channel, _payload},
            %@module{} = consumer
          ) do
        debug(fn -> "Notification for stream: #{channel}" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, consumer}
      end

      @impl GenServer
      def handle_info({:reminder}, %@module{} = consumer) do
        debug(fn -> "Reminder" end)

        GenServer.cast(self(), {:request_events})

        {:noreply, consumer}
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
