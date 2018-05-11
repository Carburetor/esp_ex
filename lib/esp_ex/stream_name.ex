defmodule EspEx.StreamName do
  alias   StreamName
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

  @enforce_keys [:category]
  defstruct(category: "", identifier: nil, types: [])

  @doc """
  Creates a new StreamName struct.

  ## Examples

      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      %EspEx.StreamName{category: "campaign",
                        identifier: "123",
                        types: :ordsets.from_list(["command", "position"])}
  """
  @spec new(category: String.t(), identifier: String.t(), types: list(String.t())) ::
          %EspEx.StreamName{
            category: String.t(),
            identifier: String.t(),
            types: list(String.t())
          }
  def new(category, identifier \\ nil, types \\ [])
      when (is_bitstring(category) and is_nil(identifier)) or is_bitstring(identifier) do
    category = String.trim(category)
    types = Enum.map(types, fn x -> String.trim(x) end)

    if category == "" do
      raise ArgumentError, message: "category must not be blank"
    else
      %__MODULE__{category: category, identifier: identifier, types: :ordsets.from_list(types)}
    end
  end

  @doc """
  Creates a StreamName struct with a provided string as an arguement.

  ## Examples
      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      %EspEx.StreamName{category: "campaign",
                        identifier: "123",
                        types: :ordsets.from_list(["command", "position"])}
  """
  @spec from_string(String) :: %EspEx.StreamName{
          category: String.t(),
          identifier: String.t(),
          types: list(String.t())
        }
  def from_string(string) do
    category = category_extractor(string)
    identifier = identifier_extractor(string)
    types = types_extractor(string, category, identifier)

    new(category, identifier, types)
  end

  defp category_extractor(string) do
    String.split(string, ":")
    |> List.first()
    |> String.split("-")
    |> List.first()
  end

  defp identifier_extractor(string) do
    identifier = Regex.run(~r/-(.+)/, string)

    if identifier == nil do
      nil
    else
      List.last(identifier)
    end
  end

  defp types_extractor(string, category, identifier) do
    types =
      String.trim_leading(string, "#{category}")
      |> String.trim_leading(":")
      |> String.trim_trailing("-#{identifier}")
      |> String.trim_trailing("+")
      |> String.split("+")

    if types == [""] do
      :ordsets.new()
    else
      :ordsets.from_list(types)
    end
  end

  @doc """
  Returns a string when provided with a StreamName struct.

  ## Examples

      iex> map = %EspEx.StreamName{category: "campaign", identifier: "123", types: :ordsets.from_list(["command", "position"])}
      iex> EspEx.StreamName.to_string(map)
      "campaign:command+position-123"
  """

  defimpl String.Chars, for: EspEx.StreamName do

    # @spec to_string(%EspEx.StreamName{
    #         category: String.t(),
    #         identifier: String.t(),
    #         types: list(String.t())
    #       }) :: String.t()
    def to_string(map) do
      identifier = identifier_checker(map)
      types = types_checker(map)

      "#{map.category}#{types}#{identifier}"
    end

    defp identifier_checker(map) do
      if map == [] do
        ""
      else
        "-#{map.identifier}"
      end
    end

    defp types_checker(map) do
      if map.types == [] do
        ""
      else
        ":#{Enum.join(map.types, "+")}"
      end
    end
  end

  @doc """
  Returns `true` if provided list is in provided map's types.

  ## Examples

      iex> map = %EspEx.StreamName{category: "campaign", identifier: nil, types: :ordsets.from_list(["command", "position"])}
      iex> list = ["command", "position"]
      iex> EspEx.StreamName.has_all_types(map, list)
      true
  """
  @spec has_all_types(
          %EspEx.StreamName{category: String.t(), identifier: String.t(), types: list(String.t())},
          list(String.t())
        ) :: boolean()
  def has_all_types(map, list) do
    list
    |> :ordsets.from_list()
    |> :ordsets.is_subset(map.types)
  end

  @doc """
  Returns `true` if StreamName struct has no identifier, but has a types.
  Returns `false` if StreamName struct has an identifier.
  ## Examples

      iex> map = %EspEx.StreamName{category: "campaign", identifier: 123, types: :ordsets.from_list(["command", "position"])}
      iex> EspEx.StreamName.is_category(map)
      false
  """
  @spec is_category(%EspEx.StreamName{
          category: String.t(),
          identifier: String.t(),
          types: list(String.t())
        }) :: boolean()
  def is_category(%__MODULE__{identifier: nil}), do: true
  def is_category(%__MODULE__{}), do: false

  @doc """
  Returns a string of the StreamName with the position appended to the end.

  ## Examples

      iex> map = %EspEx.StreamName{category: "campaign", identifier: 123, types: :ordsets.from_list(["command", "position"])}
      iex> EspEx.StreamName.position_identifier(map, 1)
      "campaign:command+position-123/1"
  """
  @spec position_identifier(
          %EspEx.StreamName{category: String.t(), identifier: String.t(), types: list(String.t())},
          integer()
        ) :: boolean()
  def position_identifier(map, position) do
    cond do
      position < 0 ->
        raise ArgumentError, message: "position must not be less than 0"

      is_float(position) ->
        raise ArgumentError, message: "position must not be a float"

      is_nil(position) ->
        to_string(map)

      true ->
        to_string(map) <> "/#{position}"
    end
  end
end
