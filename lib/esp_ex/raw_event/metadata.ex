defmodule EspEx.RawEvent.Metadata do
  @schema_version "1.0"

  @type t :: %EspEx.RawEvent.Metadata{}
  defstruct causation_message_stream_name: nil,
            causation_message_position: nil,
            causation_message_global_position: nil,
            correlation_stream_name: nil,
            reply_stream_name: nil,
            schema_version: @schema_version
end
