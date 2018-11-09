defmodule Delugex.Event.Metadata do
  @schema_version "1.0"

  @type t :: %Delugex.Event.Metadata{}
  defstruct causation_message_stream_name: nil,
            causation_message_position: nil,
            causation_message_global_position: nil,
            correlation_stream_name: nil,
            reply_stream_name: nil,
            schema_version: @schema_version

  defimpl Jason.Encoder do
    def encode(value, opts) do
      value
      |> Map.from_struct()
      |> Jason.Encode.map(opts)
    end
  end
end
