defmodule EspEx.Event.Unknown do
  @type t :: %EspEx.Event.Unknown{raw_event: EspEx.RawEvent.t()}

  @enforce_keys [:raw_event]
  defstruct [:raw_event]

  defimpl EspEx.Event.Transformable do
    def type(_event), do: "Unknown"
    def to_raw_event(%{raw_event: raw_event}), do: raw_event
    def to_raw_event(%{raw_event: raw_event}, _), do: raw_event
  end
end
