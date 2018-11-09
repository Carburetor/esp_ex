defmodule Delugex.Event.Raw do
  @moduledoc """
  Representation of an event in memory
  """

  @typedoc """
  - `:data` must be a map, not a struct
  """
  @type t :: %Delugex.Event.Raw{
          id: String.t(),
          stream_name: Delugex.StreamName.t(),
          type: String.t(),
          position: non_neg_integer | nil,
          global_position: non_neg_integer | nil,
          data: map(),
          metadata: Delugex.Event.Metadata.t(),
          time: NaiveDateTime.t() | nil
        }

  defstruct id: nil,
            stream_name: nil,
            type: nil,
            position: nil,
            global_position: nil,
            data: %{},
            metadata: %Delugex.Event.Metadata{},
            time: nil

  def next_position(nil), do: 0
  def next_position(position) when is_integer(position), do: position + 1

  def next_global_position(nil), do: 0

  def next_global_position(global_position) when is_integer(global_position) do
    global_position + 1
  end
end
