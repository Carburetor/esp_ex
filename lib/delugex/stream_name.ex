defmodule Delugex.StreamName do
  @moduledoc """
  StreamName is a module to manage the location where events are written.
  Stream names could be intended as URLs for where events are located.
  The StreamName protocol provides an easy way to access the data that
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

  The struct coming out of `build` should look like:
  %StreamName{category: "campaign", id: "123", types: ["command", "position"]}
  The function `to_string` should convert it back to
  `campaign:command+position-123`
  """

  alias Delugex.StreamName.Reader

  @type category :: Reader.category()
  @type id :: Reader.id()
  @type t :: Reader.t()

  @doc "Converts a StreamName into a string to be supplied to the database"
  @spec to_string(stream_name :: t()) :: String.t()
  defdelegate to_string(stream_name), to: Reader

  @doc "Extracts category from a StreamName"
  @spec category(stream_name :: t()) :: category()
  defdelegate category(stream_name), to: Reader

  @doc "Extracts id from a StreamName"
  @spec id(stream_name :: t()) :: id()
  defdelegate id(stream_name), to: Reader

  @doc "true if the stream_name has nil id"
  @spec category?(stream_name :: t()) :: boolean()
  defdelegate category?(stream_name), to: Reader

  @doc "true if two streams are the same"
  @spec equal?(left :: t(), right :: t()) :: boolean()
  def equal?(left, right) do
    category(left) == category(right) && id(left) == id(right)
  end

  @doc "true if stream names are the same or right one is category of left one"
  @spec subset?(left :: t(), right :: t()) :: boolean()
  def subset?(left, right) do
    case category?(right) do
      true -> category(right) == category(left)
      false -> equal?(left, right)
    end
  end
end
