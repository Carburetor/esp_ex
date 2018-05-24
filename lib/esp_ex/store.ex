defmodule EspEx.Store do
  @moduledoc """
  Builds the latest version of an entity based on its stream
  """

  alias EspEx.Logger

  @type fetch_opts :: [batch_size: pos_integer()]
  @callback fetch(
              identifier :: EspEx.StreamName.id(),
              opts :: fetch_opts
            ) :: {struct(), EspEx.MessageStore.version() | nil}

  @doc """
  - `:message_store` **required** `EspEx.MessageStore` implementation, used to read
    events
  - `:entity_builder` **required** module implementing `EspEx.Entity` behaviour
  - `:event_transformer` **required** implementation of
    `EspEx.EventTransformer`
  - `:projection` **required** implementation of `EspEx.Store`
  - `:stream_name` **required** a category stream, struct from
    `EspEx.StreamName`
  """
  defmacro __using__(opts \\ []) do
    message_store = Keyword.get(opts, :message_store)
    entity_builder = Keyword.get(opts, :entity_builder)
    event_transformer = Keyword.get(opts, :event_transformer)
    projection = Keyword.get(opts, :projection)
    stream_name = Keyword.get(opts, :stream_name)

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def fetch(identifier \\ nil, opts \\ []) do
        stream = unquote(stream_name)

        stream =
          case identifier do
            nil -> stream
            _ -> Map.put(stream, :identifier, identifier)
          end

        unquote(__MODULE__).fetch(
          unquote(message_store),
          unquote(entity_builder),
          unquote(event_transformer),
          unquote(projection),
          stream,
          opts
        )
      end
    end
  end

  @spec fetch(
          message_store :: module,
          entity_builder :: module,
          event_transformer :: module,
          projection :: module,
          stream_name :: EspEx.StreamName.t(),
          opts :: fetch_opts()
        ) :: {struct(), EspEx.MessageStore.version() | nil}
  def fetch(
        message_store,
        entity_builder,
        event_transformer,
        projection,
        %EspEx.StreamName{} = stream_name,
        opts \\ []
      )
      when is_atom(message_store) and is_atom(entity_builder) and
             is_atom(event_transformer) and is_atom(projection) do
    new_ent = entity_builder.new()
    batch_size = Keyword.get(opts, :batch_size, 10)

    message_store.stream(stream_name, 0, batch_size)
    |> Stream.map(event_and_position(event_transformer))
    |> Enum.reduce({new_ent, nil}, fn {event, position}, {entity, _} ->
      Logger.debug(fn ->
        "Applying #{event.__struct__} to #{entity.__struct__} [##{position}]"
      end)

      entity = projection.apply(entity, event)
      {entity, position}
    end)
    |> maybe_mark_empty
  end

  defp maybe_mark_empty({_, nil}), do: {nil, nil}
  defp maybe_mark_empty({_, _} = result), do: result

  defp event_and_position(event_transformer) do
    fn raw_event ->
      {event_transformer.to_event(raw_event), raw_event.position}
    end
  end
end
