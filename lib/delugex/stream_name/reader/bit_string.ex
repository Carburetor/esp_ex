defimpl Delugex.StreamName.Reader, for: BitString do
  def to_string(stream_name), do: stream_name

  def category(stream_name) do
    stream_name
    |> String.split("-")
    |> List.first()
  end

  def id(stream_name) do
    stream_category = Delugex.StreamName.category(stream_name)

    id =
      stream_name
      |> String.trim_leading(stream_category)
      |> String.trim_leading("-")

    case id do
      "" -> nil
      _ -> id
    end
  end

  def category?(stream_name) do
    stream_id = Delugex.StreamName.id(stream_name)

    is_nil(stream_id)
  end
end
