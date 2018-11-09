defmodule Delugex.MessageStore.Stream.Position do
  @moduledoc false
  defstruct [:reader, :stream_name, :position, :batch_size]

  def new(reader, stream_name, position, batch_size) do
    %__MODULE__{
      reader: reader,
      stream_name: stream_name,
      position: position,
      batch_size: batch_size
    }
  end
end
