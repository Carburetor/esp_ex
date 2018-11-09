defmodule Delugex.MessageStore.Stream do
  @moduledoc false

  alias Delugex.MessageStore.Stream.Position

  def from_position(%Position{} = stream_position) do
    Stream.resource(fn -> stream_position end, &next/1, &finish/1)
  end

  defp next(%Position{position: nil} = stream_pos), do: {:halt, stream_pos}

  defp next(%Position{} = stream_position) do
    events = read_batch_from(stream_position)
    new_stream_position = next_stream_position(stream_position, events)
    {events, new_stream_position}
  end

  defp finish(%Position{}), do: nil

  defp next_stream_position(%Position{} = stream_position, []) do
    Map.put(stream_position, :position, nil)
  end

  defp next_stream_position(%Position{} = stream_position, events) do
    %{position: position} = List.last(events)
    Map.put(stream_position, :position, position + 1)
  end

  defp read_batch_from(%Position{
         reader: reader,
         stream_name: stream_name,
         position: position,
         batch_size: batch_size
       }) do
    reader.read_batch(stream_name, position, batch_size)
  end
end
