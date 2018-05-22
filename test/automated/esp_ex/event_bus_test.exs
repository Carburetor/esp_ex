defmodule EspEx.EventBusTest do
  use ExUnit.Case, async: true

  alias EspEx.EventBus.Static, as: EventBus
  alias EspEx.RawEvent
  alias EspEx.StreamName

  @stream_name %StreamName{category: "campaign", identifier: "123", types: []}
  @empty_stream %StreamName{category: "empty", identifier: nil, types: []}
  @raw_event %RawEvent{
    id: "11111111",
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Unnamed"}
  }

  describe "EventBus.write_initial!" do
    test "doesn't raises on first message" do
      EventBus.write_initial!(@raw_event)
    end
  end

  describe "EventBus.write!" do
    test "returns 3" do
      version = EventBus.write!(@raw_event)

      assert version == 3
    end
  end

  describe "EventBus.read_last" do
    test "returns %RawEvent when stream has events" do
      raw_event = EventBus.read_last(@stream_name)

      assert raw_event.position == 2 && raw_event.id == "uuid"
    end

    test "returns nil when stream is empty" do
      raw_event = EventBus.read_last(@empty_stream)

      assert raw_event == nil
    end
  end

  describe "EventBus.read_batch" do
    test "returns list of %RawEvent when stream has events" do
      raw_events = EventBus.read_batch(@stream_name)

      assert length(raw_events) == 3
    end

    test "returns [] when reading after last event" do
      raw_events = EventBus.read_batch(@stream_name, 3)

      assert raw_events == []
    end

    test "returns list of %RawEvent with max size of batch_size" do
      raw_events = EventBus.read_batch(@stream_name, 0, 2)

      assert length(raw_events) == 2
    end
  end

  describe "EventBus.read_version" do
    test "returns last event position" do
      version = EventBus.read_version(@stream_name)

      assert version == 2
    end

    test "returns nil when stream is empty" do
      version = EventBus.read_version(@empty_stream)

      assert version == nil
    end
  end

  describe "EventBus.stream" do
    test "streams raw events as read from batch" do
      raw_events = EventBus.stream(@stream_name) |> Enum.to_list()

      assert length(raw_events) == 3
    end

    test "streams raw events as read from batch even in different chunks" do
      raw_events = EventBus.stream(@stream_name, 1, 1) |> Enum.to_list()

      assert length(raw_events) == 2
    end
  end
end
