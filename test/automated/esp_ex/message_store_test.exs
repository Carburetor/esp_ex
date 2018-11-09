defmodule EspEx.MessageStoreTest do
  use ExUnit.Case, async: true

  alias EspEx.MessageStore.Static, as: MessageStore
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

  describe "MessageStore.write_initial!" do
    test "doesn't raises on first message" do
      MessageStore.write_initial!(@raw_event)
    end
  end

  describe "MessageStore.write!" do
    test "returns 3" do
      version = MessageStore.write!(@raw_event)

      assert version == 3
    end
  end

  describe "MessageStore.read_last" do
    test "returns %RawEvent when stream has events" do
      raw_event = MessageStore.read_last(@stream_name)

      assert raw_event.position == 2 && raw_event.id == "uuid"
    end

    test "returns nil when stream is empty" do
      raw_event = MessageStore.read_last(@empty_stream)

      assert raw_event == nil
    end
  end

  describe "MessageStore.read_batch" do
    test "returns list of %RawEvent when stream has events" do
      raw_events = MessageStore.read_batch(@stream_name)

      assert length(raw_events) == 3
    end

    test "returns [] when reading after last event" do
      raw_events = MessageStore.read_batch(@stream_name, 3)

      assert raw_events == []
    end

    test "returns list of %RawEvent with max size of batch_size" do
      raw_events = MessageStore.read_batch(@stream_name, 0, 2)

      assert length(raw_events) == 2
    end
  end

  describe "MessageStore.read_version" do
    test "returns last event position" do
      version = MessageStore.read_version(@stream_name)

      assert version == 2
    end

    test "returns nil when stream is empty" do
      version = MessageStore.read_version(@empty_stream)

      assert version == nil
    end
  end

  describe "MessageStore.stream" do
    test "streams raw events as read from batch" do
      raw_events = MessageStore.stream(@stream_name) |> Enum.to_list()

      assert length(raw_events) == 3
    end

    test "streams raw events as read from batch even in different chunks" do
      raw_events = MessageStore.stream(@stream_name, 1, 1) |> Enum.to_list()

      assert length(raw_events) == 2
    end
  end
end
