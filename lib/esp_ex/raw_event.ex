defmodule EspEx.RawEvent do
  @moduledoc """
  Representation of an event in memory
  """

  @typedoc """
  - `:data` must be a map, not a struct
  """
  @type t :: %EspEx.RawEvent{
          id: String.t(),
          stream_name: EspEx.StreamName.t(),
          type: String.t(),
          position: non_neg_integer | nil,
          global_position: non_neg_integer | nil,
          data: map(),
          metadata: EspEx.RawEvent.Metadata.t(),
          time: NaiveDateTime.t() | nil
        }

  defstruct id: nil,
            stream_name: nil,
            type: nil,
            position: nil,
            global_position: nil,
            data: %{},
            metadata: %EspEx.RawEvent.Metadata{},
            time: nil

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

  def next_position(nil), do: 0
  def next_position(position) when is_integer(position), do: position + 1

  def next_global_position(nil), do: 0

  def next_global_position(global_position) when is_integer(global_position) do
    global_position + 1
  end

  defp to_nil_or_str(nil), do: nil
  defp to_nil_or_str(value), do: to_string(value)
end
