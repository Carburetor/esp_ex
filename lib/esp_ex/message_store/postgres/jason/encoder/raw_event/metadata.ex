defimpl Jason.Encoder, for: EspEx.RawEvent.Metadata do
  def encode(value, opts) do
    value
    |> Map.from_struct()
    |> Jason.Encode.map(opts)
  end
end
