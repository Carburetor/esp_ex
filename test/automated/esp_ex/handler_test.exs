defmodule EspEx.HandlerTest do
  use ExUnit.Case, async: true

  alias EspEx.RawEvent
  alias EspEx.StreamName

  @stream_name %StreamName{category: "campaign", identifier: "123", types: []}
  @raw_event %RawEvent{
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
    use EspEx.Handler

    @impl EspEx.Handler
    def handle(%Renamed{}, _, _) do
      send(self(), {:renamed})
    end
  end

  describe "Handler.handle" do
    test "defaults to doing nothing" do
      PersonHandler.handle(%Nopped{}, @raw_event, nil)
    end

    test "calls specific handle when implemented" do
      PersonHandler.handle(%Renamed{}, @raw_event, nil)

      assert_receive({:renamed})
    end
  end
end
