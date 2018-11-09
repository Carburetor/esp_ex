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
  %StreamName{category: "campaign", identifier: "123", types: stream_nameSet<"command",
  "position">}
  The function `to_string` should convert it back to
  `campaign:command+position-123`
  """

  @type id :: String.t() | nil
  @type t :: %EspEx.StreamName{
          category: String.t(),
          identifier: id(),
          types: list(String.t())
        }

  @enforce_keys [:category]
  defstruct(category: "", identifier: nil, types: :ordsets.new())

  def empty, do: %__MODULE__{category: ""}

  @doc """
  Creates a new StreamName struct.

  ## Examples

      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      %EspEx.StreamName{category: "campaign",
                        identifier: "123",
                        types: :ordsets.from_list(["command", "position"])}
  """

  def new(category), do: new(category, nil, [])
  def new(category, identifier), do: new(category, identifier, [])
  def new("", _, _), do: raise(ArgumentError, message: "category is blank")

  @spec new(
          category :: String.t(),
          identifier :: String.t() | nil,
          types :: list(String.t())
        ) :: EspEx.StreamName.t()
  def new(category, identifier, types)
      when is_bitstring(category) and
             (is_nil(identifier) or is_bitstring(identifier)) and is_list(types) do
    category = String.trim(category)
    category_empty!(category)
    identifier = trim_or_nil(identifier)
    types = Enum.map(types, &String.trim/1)

    %__MODULE__{
      category: category,
      identifier: identifier,
      types: :ordsets.from_list(types)
    }
  end

  defp trim_or_nil(nil), do: nil
  defp trim_or_nil(identifier), do: String.trim(identifier)

  @doc """
  Creates a StreamName struct with a provided string as an arguement.

  ## Examples
      iex> EspEx.StreamName.from_string("campaign:command+position-123")
      %EspEx.StreamName{category: "campaign",
                        identifier: "123",
                        types: :ordsets.from_list(["command", "position"])}
  """
  @spec from_string(text :: String.t()) :: EspEx.StreamName.t()
  def from_string(text) when is_bitstring(text) do
    category = extract_category(text)
    category_empty!(category)
    identifier = extract_identifier(text)
    types = extract_types(text, category, identifier)

    new(category, identifier, types)
  end

  defp category_empty!(""), do: raise(ArgumentError, "Category is blank")
  defp category_empty!(_), do: nil

  defp extract_category(string) do
    String.split(string, ":")
    |> List.first()
    |> String.split("-")
    |> List.first()
    |> String.trim()
  end

  defp extract_identifier(string) do
    identifier = Regex.run(~r/-(.+)/, string)

    if identifier == nil do
      nil
    else
      List.last(identifier)
    end
  end

  defp extract_types(string, category, identifier) do
    types =
      string
      |> String.trim_leading(category)
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

  defimpl String.Chars do
    @doc """
    Returns a string when provided with a StreamName struct.

    ## Examples

        iex> stream_name = %EspEx.StreamName{category: "campaign", identifier: "123", types: :ordsets.from_list(["command", "position"])}
        iex> EspEx.StreamName.to_string(stream_name)
        "campaign:command+position-123"
    """
    @spec to_string(stream_name :: EspEx.StreamName.t()) :: String.t()
    def to_string(%EspEx.StreamName{
          category: category,
          identifier: identifier,
          types: types
        }) do
      identifier = identifier_to_string(identifier)
      types = types_to_string(types)

      "#{category}#{types}#{identifier}"
    end

    defp identifier_to_string(nil), do: ""
    defp identifier_to_string(identifier), do: "-#{identifier}"

    defp types_to_string([]), do: ""
    defp types_to_string(types), do: ":#{Enum.join(types, "+")}"
  end

  @doc """
  Returns `true` if provided list is in provided stream_name's types.

  ## Examples

      iex> stream_name = %EspEx.StreamName{category: "campaign", identifier: nil, types: :ordsets.from_list(["command", "position"])}
      iex> list = ["command", "position"]
      iex> EspEx.StreamName.has_all_types(stream_name, list)
      true
  """
  @spec has_all_types(
          stream_name :: EspEx.StreamName.t(),
          list :: list(String.t())
        ) :: boolean()
  def has_all_types(%__MODULE__{types: types}, list) do
    list
    |> :ordsets.from_list()
    |> :ordsets.is_subset(types)
  end

  @doc """
  Returns `true` if StreamName struct has no identifier, but has a types.
  Returns `false` if StreamName struct has an identifier.
  ## Examples

      iex> stream_name = %EspEx.StreamName{category: "campaign", identifier: 123, types: :ordsets.from_list(["command", "position"])}
      iex> EspEx.StreamName.category?(stream_name)
      false
  """
  @spec category?(stream_name :: EspEx.StreamName.t()) :: boolean()
  def category?(%__MODULE__{identifier: nil}), do: true
  def category?(%__MODULE__{}), do: false

  @doc """
  Returns a string of the StreamName with the position appended to the end.

  ## Examples

      iex> stream_name = %EspEx.StreamName{category: "campaign", identifier: 123, types: :ordsets.from_list(["command", "position"])}
      iex> EspEx.StreamName.position_identifier(stream_name, 1)
      "campaign:command+position-123/1"
  """
  def position_identifier(%__MODULE__{} = stream_name, nil) do
    to_string(stream_name)
  end

  @spec position_identifier(
          stream_name :: EspEx.StreamName.t(),
          position :: non_neg_integer() | nil
        ) :: String.t()
  def position_identifier(%__MODULE__{} = stream_name, position)
      when is_integer(position) and position >= 0 do
    to_string(stream_name) <> "/#{position}"
  end

  @spec subset?(
          stream_name :: EspEx.StreamName.t(),
          other_stream :: EspEx.StreamName.t()
        ) :: boolean
  def subset?(%__MODULE__{} = stream_name, %__MODULE__{} = other_stream) do
    case category?(other_stream) do
      true ->
        stream_name.category == other_stream.category &&
          stream_name.types == other_stream.types

      false ->
        stream_name == other_stream
    end
  end
end
