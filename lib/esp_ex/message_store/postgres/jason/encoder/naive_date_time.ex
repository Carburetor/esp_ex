defimpl Jason.Encoder, for: NaiveDateTime do
  def encode(value, opts) do
    NaiveDateTime.to_iso8601(value)
  end
end
