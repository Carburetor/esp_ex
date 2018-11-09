defmodule Delugex.StreamName.Decoder do
  @callback decode(stream_name :: String.t()) :: Delugex.StreamName.t()
end
