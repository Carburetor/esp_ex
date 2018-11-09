defmodule Delugex.Event.Unknown do
  @type t :: %Delugex.Event.Unknown{raw: Delugex.Event.Raw.t()}

  @enforce_keys [:raw]
  defstruct [:raw]

  defimpl Delugex.Event.Transformable do
    def type(_event), do: "Unknown"
    def to_event(%{raw: raw}), do: raw
    def to_event(%{raw: raw}, _), do: raw
  end
end
