defmodule Delugex.Stream.Name do
  @behaviour Delugex.StreamName.Decoder

  alias __MODULE__

  @moduledoc """
  Stream.Name is a module to manage the location where events are written.
  Stream names could be intended as URLs for where events are located.
  The struct provides an easy way to access the data that
  otherwise would be in a String.
  Stream names are **camelCased**.
  A full stream name might look like: `user:command+position-123`.

  - `user` is the stream name **category**
  - category is required
  - `command` and `position` are the stream **types**
  - `123` is the stream `id` (string, will be UUID)
  - id is optional
  - If the stream name has no `id`, the dash must be omitted
  - Any dash after the first dash are considered part of the id
  - If the stream has no types, `:` must be omitted
  - Types must be separated by the `+` sign
  - types are optional

  The struct returned by `build` should look like:
  %Stream.Name{category: "campaign", id: "123", types: ["command", "position"]}
  The function `to_string` should convert it back to
  `campaign:command+position-123`
  """

  @type t :: %Name{
          category: Delugex.StreamName.category(),
          id: Delugex.StreamName.id()
        }

  defstruct(category: "", id: nil)

  @spec new(
          category :: Delugex.StreamName.category(),
          id :: Delugex.StreamName.id()
        ) :: t()
  def new(category, id \\ nil)
      when is_binary(category) and (is_nil(id) or is_binary(id)) do
    %__MODULE__{category: category, id: id}
  end

  @doc """
  Creates a Name struct with a provided string as an arguement.

  ## Examples
      iex> Name.decode("campaign:command+position-123")
      %Name{category: "campaign",
                        id: "123",
                        types: ["command", "position"]}
  """
  @impl Delugex.StreamName.Decoder
  @spec decode(text :: String.t()) :: Name.t()
  def decode(text) when is_binary(text) do
    category = extract_category(text)
    id = extract_id(text, category)

    new(category, id)
  end

  defp extract_category(string) do
    string
    |> String.split("-")
    |> List.first()
  end

  defp extract_id(string, category) do
    id =
      string
      |> trim_prefix(category)
      |> trim_prefix("-")

    case id do
      "" -> nil
      _ -> id
    end
  end

  defp trim_prefix(string, ""), do: string
  defp trim_prefix(string, match), do: String.replace_prefix(string, match, "")

  defimpl String.Chars do
    @spec to_string(stream_name :: Name.t()) :: String.t()
    def to_string(%Name{category: category, id: id}) do
      id = id_to_string(id)

      "#{category}#{id}"
    end

    defp id_to_string(nil), do: ""
    defp id_to_string(id), do: "-#{id}"
  end

  defimpl Delugex.StreamName.Reader do
    def to_string(%Name{} = stream_name), do: Kernel.to_string(stream_name)
    def category(%Name{category: category}), do: category
    def id(%Name{id: id}), do: id
    def category?(%Name{id: nil}), do: true
    def category?(%Name{id: id}) when not is_nil(id), do: false
  end

  defimpl Jason.Encoder do
    def encode(value, opts) do
      value
      |> Delugex.StreamName.to_string()
      |> Jason.Encode.map(opts)
    end
  end
end
