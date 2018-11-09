defmodule Delugex.HandlerTest do
  use Delugex.Case

  alias Delugex.Event.Raw
  alias Delugex.Stream.Name

  @stream_name %Name{category: "campaign", id: "123"}
  @raw %Raw{
    id: "11111111",
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Unnamed"}
  }

  defmodule Nopped do
    defstruct []
  end

  defmodule Renamed do
    defstruct []
  end

  defmodule PersonHandler do
    use Delugex.Handler

    @impl Delugex.Handler
    def handle(%Renamed{}, _, _) do
      send(self(), {:renamed})
    end
  end

  describe "Handler.handle" do
    test "defaults to doing nothing" do
      PersonHandler.handle(%Nopped{}, @raw, nil)
    end

    test "calls specific handle when implemented" do
      PersonHandler.handle(%Renamed{}, @raw, nil)

      assert_receive({:renamed})
    end
  end
end
