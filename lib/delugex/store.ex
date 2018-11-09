defmodule Delugex.Store do
  @moduledoc """
  Builds the latest version of an entity based on its stream
  """

  alias Delugex.Logger

  @type fetch_opts :: [batch_size: pos_integer()]
  @callback fetch(
              identifier :: Delugex.StreamName.id(),
              opts :: fetch_opts
            ) :: {struct(), Delugex.MessageStore.optional_version()}

  @doc """
  - `:message_store` **required** `Delugex.MessageStore` implementation, used to read
    events
  - `:event_transformer` **required** implementation of
    `Delugex.EventTransformer`
  - `:projection` **required** implementation of `Delugex.Store`
  - `:stream_name` **required** a category stream, struct from
    `Delugex.StreamName`
  """
  defmacro __using__(opts \\ []) do
    # opts = Macro.escape(opts)

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def fetch(identifier \\ nil, opts \\ []) do
        stream = unquote(opts)[:stream_name]

        stream =
          case identifier do
            nil -> stream
            _ -> Map.put(stream, :id, identifier)
          end

        unquote(__MODULE__).fetch(
          unquote(opts)[:message_store],
          unquote(opts)[:event_transformer],
          unquote(opts)[:projection],
          stream,
          opts
        )
      end
    end
  end

  @spec fetch(
          message_store :: module,
          event_transformer :: module,
          projection :: module,
          stream_name :: Delugex.StreamName.t(),
          opts :: fetch_opts()
        ) :: {struct(), Delugex.MessageStore.optional_version()}
  def fetch(
        message_store,
        event_transformer,
        projection,
        stream_name,
        opts \\ []
      )
      when is_atom(message_store) and is_atom(event_transformer) and
             is_atom(projection) do
    batch_size = Keyword.get(opts, :batch_size, 10)

    message_store.stream(stream_name, 0, batch_size)
    |> Stream.map(event_and_position(event_transformer))
    |> Enum.reduce({nil, nil}, fn {event, position}, {entity, _} ->
      Logger.debug(fn ->
        "Applying #{event.__struct__} [##{position}]"
      end)

      entity = projection.apply(entity, event)
      {entity, position}
    end)
    |> maybe_mark_empty
  end

  defp maybe_mark_empty({_, nil}), do: {nil, nil}
  defp maybe_mark_empty({_, _} = result), do: result

  defp event_and_position(event_transformer) do
    fn raw ->
      {event_transformer.transform(raw), raw.position}
    end
  end
end
