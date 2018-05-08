defmodule EspEx.StreamName do
  alias StreamName
  @moduledoc """
  A StreamName is a module to manage the location where events are written.
  Think of stream names as a URL for where your events are located.
  The StreamName struct provides an easy way to access the data that otherwise
  would be in a String, which would require always validation and take more
  time to extract the relevant information out of it.
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

  # This enforces the category key as a requirement
  @enforce_keys [:category]
  defstruct(category: "", identifier: nil, types: MapSet.new())


  # TODO: String.trim everyting to remove white space
  def new(category, identifier \\ nil, types \\ []) do
    cond do
      String.trim(category) == "" ->
        raise ArgumentError, message: "category must not be blank"
      category == nil ->
        raise FunctionClauseError, message: "category must not be nil"
      true ->
        %__MODULE__{category: category,
                    identifier: identifier,
                    types: MapSet.new(types)
                    }
    end
  end

  @doc """
  from_string

  ## Examples

      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      %EspEx.StreamName{category: "campaign",
                        identifier: "123",
                        types: MapSet.new(["command", "position"])}
  """
  def from_string(string) do
    category = category_checker(string)
    identifier = identifier_checker(string)
    types = types_checker(string)

    new(category, identifier, types)
  end

  defp category_checker(string) do
    String.split(string, ":")
    |> List.first
    |> String.split("-")
    |> List.first

  end

  defp identifier_checker(string) do

    result = Regex.run(~r/-(.+)/, string)

    if result == nil do
      nil
    else
      List.last(result)
    end
  end

  defp types_checker(string) do
    clean_string = Regex.run(~r/:(.+)/, string)

    if clean_string == nil do
      MapSet.new([])
    else
      x = clean_string |> List.last
      result = String.split(x, "-") |> List.first
      result2 = String.split(result, ":") |> List.last

      if result2 == "" do
        MapSet.new([])
      else
        list = String.split(result2, "+")
        Enum.filter(list, fn(x) -> x != "" end)
        |> MapSet.new()
      end
    end
  end

  @doc """
  from_string

  ## Examples

      iex> map = %EspEx.StreamName{category: "campaign", identifier: "123", types: MapSet.new(["command", "position"])}
      iex> EspEx.StreamName.to_string(map)
      "campaign:command+position-123"

      iex> map = %EspEx.StreamName{category: "campaign", types: MapSet.new(["command", "position"])}
      iex> EspEx.StreamName.to_string(map)
      "campaign:command+position"

      iex> map = %EspEx.StreamName{category: "campaign"}
      iex> EspEx.StreamName.to_string(map)
      "campaign"

      iex> map = %EspEx.StreamName{category: "campaign", identifier: "123"}
      iex> EspEx.StreamName.to_string(map)
      "campaign-123"
  """
  def to_string(map) do
    category = map.category
    identifier = map.identifier

    x = MapSet.to_list(map.types)
    types = Enum.join(x, "+")

    "#{category}" <> ":#{types}" <> "-#{identifier}"
  end

end
