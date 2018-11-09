defimpl Jason.Encoder, for: EspEx.StreamName do
  def encode(value, opts) do
    value
    |> to_string()
    |> Jason.Encode.map(opts)
  end
end
