defmodule EspEx.StreamName do
  alias StreamName
  @moduledoc """
  A StreamName is a module to manage the location where events are written.
  Think of stream names as a URL for where your events are located.
  The StreamName struct provides an easy way to access the data that otherwise
  would be in a String, which would require always validation and take more
xtract the relevant information out of it.
  Stream names are **camelCased**.
  Sometimes we refer to "Streams" but we actually mean "Stream names".
  A full stream name might look like: `campaign:command+position-123`.

  - `campaign` is the stream name **category**
  - category is required
  - `command` and `position` are the stream **types**
  - `123` is the stream `identifier` (string, will be UUID)
  - identifier is optional
  - If the stream name has no `identifier`, the dash must be omitted
  - Any dash after the first dash are considered part of the identifier
  - If the stream has no types, `:` must be omitted
  - Types must be separated by the `+` sign and must always be sorted
  - types are optional

  The struct coming out of `from_string` should look like:
  %StreamName{category: "campaign", identifier: "123", types: MapSet<"command",
  "position">}
  The function `to_string` should convert it back to
  `campaign:command+position-123`
  """

  defstruct(category: "", identifier: nil, types: {})

  def new(category, identifier \\ nil, types \\ {}) do
    %EspEx.StreamName{category: category, identifier: identifier, types: types}
  end

  @doc """
  from_string

  ## Examples

      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      ["campaign"]

  """

  def from_string(string) do
    category_string = Regex.run(~r/^\w+/, string)

  end
end
