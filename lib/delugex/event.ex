defmodule Delugex.Event do
  @typedoc """
  - `:data` must be a map, not a struct
  """
  @type t :: %__MODULE__{
          id: String.t(),
          stream_name: Delugex.StreamName.t(),
          type: String.t(),
          data: map(),
          metadata: Delugex.Event.Metadata.t()
        }

  defstruct id: nil,
            stream_name: nil,
            type: nil,
            data: %{},
            metadata: %Delugex.Event.Metadata{}

  alias Delugex.Event.Transformable

  def to_event(term) when is_map(term) do
    Transformable.to_event(term)
  end

  @spec to_event(
          term :: any(),
          event_base :: Delugex.Event.t()
        ) :: Delugex.Event.t()
  def to_event(term, %__MODULE__{} = event_base) when is_map(term) do
    Transformable.to_event(term, event_base)
  end

  @spec to_event(
          term :: any(),
          stream_name :: Delugex.StreamName.t()
        ) :: Delugex.Event.t()
  def to_event(term, stream_name) when is_map(term) do
    Transformable.to_event(term, stream_name)
  end

  @spec type(text :: String.t()) :: String.t()
  def type(text) when is_binary(text), do: text

  @spec type(term :: struct) :: String.t()
  def type(%{__struct__: module}) do
    module
    |> to_string()
    |> Module.split()
    |> List.last()
  end

  def caused_by(%__MODULE__{} = event, %__MODULE__{} = causation) do
    meta = causation.metadata
    correlation_stream_name = to_nil_or_str(meta.correlation_stream_name)
    reply_stream_name = to_nil_or_str(meta.reply_stream_name)
    causation_message_stream_name = to_nil_or_str(causation.stream_name)

    event_meta =
      event.metadata
      |> Map.put(:correlation_stream_name, correlation_stream_name)
      |> Map.put(:reply_stream_name, reply_stream_name)
      |> Map.put(:causation_message_stream_name, causation_message_stream_name)
      |> Map.put(:causation_message_position, causation.position)
      |> Map.put(:causation_message_global_position, causation.global_position)

    Map.put(event, :metadata, event_meta)
  end

  defp to_nil_or_str(nil), do: nil
  defp to_nil_or_str(value), do: to_string(value)
end
