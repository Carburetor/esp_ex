defmodule EspEx.Handler.Unhandled do
  @moduledoc false

  defmacro __before_compile__(_env) do
    quote location: :keep do
      @impl EspEx.Handler
      def handle(event, _, _) do
        EspEx.Logger.info(fn -> "Event #{inspect(event)} unhandled" end)
      end
    end
  end
end
