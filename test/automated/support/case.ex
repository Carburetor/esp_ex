defmodule Delugex.Case do
  defmacro __using__(opts) do
    opts =
      opts
      |> Keyword.put_new(:async, true)
      |> Macro.escape()

    quote do
      use ExUnit.Case, unquote(opts)
    end
  end
end
