defmodule Delugex.Config do
  def get(module, key, default \\ nil) do
    app = Application.get_env(:delugex, module)

    subconfig =
      case app do
        nil -> []
        _ -> app
      end

    Keyword.get(subconfig, key, default)
  end
end
