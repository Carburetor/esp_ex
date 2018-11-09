defprotocol Delugex.StreamName.Reader do
  @type category :: String.t()
  @type id :: String.t() | nil
  @type t :: any()

  @doc "Converts a StreamName into a string to be supplied to the database"
  @spec to_string(stream_name :: t()) :: String.t()
  def to_string(stream_name)

  @doc "Extracts category from a StreamName"
  @spec category(stream_name :: t()) :: category()
  def category(stream_name)

  @doc "Extracts id from a StreamName"
  @spec id(stream_name :: t()) :: id()
  def id(stream_name)

  @doc "true if the stream_name has nil id"
  @spec category?(stream_name :: t()) :: boolean()
  def category?(stream_name)
end
