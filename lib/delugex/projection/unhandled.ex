defmodule Delugex.Projection.Unhandled do
  @moduledoc false

  defmacro __before_compile__(_env) do
    quote location: :keep do
      @impl Delugex.Projection
      def apply(entity, event) do
        Delugex.Logger.warn(fn ->
          "Event #{inspect(event)} ignored for entity #{inspect(entity)}"
        end)

        entity
      end
    end
  end
end
