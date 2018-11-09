defmodule Delugex.MessageStoreTest do
  use Delugex.Case

  alias Delugex.MessageStore.Static, as: MessageStore
  alias Delugex.Event.Raw
  alias Delugex.Stream.Name

  @stream_name %Name{category: "campaign", id: "123"}
  @empty_stream %Name{category: "empty", id: nil}
  @raw %Raw{
    id: "11111111",
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Unnamed"}
  }

  describe "MessageStore.write_initial!" do
    test "doesn't raises on first message" do
      MessageStore.write_initial!(@raw)
    end
  end

  describe "MessageStore.write!" do
    test "returns 3" do
      version = MessageStore.write!(@raw)

      assert version == 3
    end
  end

  describe "MessageStore.read_last" do
    test "returns %Raw when stream has events" do
      raw = MessageStore.read_last(@stream_name)

      assert raw.position == 2 && raw.id == "uuid"
    end

    test "returns nil when stream is empty" do
      raw = MessageStore.read_last(@empty_stream)

      assert raw == nil
    end
  end

  describe "MessageStore.read_batch" do
    test "returns list of %Raw when stream has events" do
      raws = MessageStore.read_batch(@stream_name)

      assert length(raws) == 3
    end

    test "returns [] when reading after last event" do
      raws = MessageStore.read_batch(@stream_name, 3)

      assert raws == []
    end

    test "returns list of %Raw with max size of batch_size" do
      raws = MessageStore.read_batch(@stream_name, 0, 2)

      assert length(raws) == 2
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
      raws = MessageStore.stream(@stream_name) |> Enum.to_list()

      assert length(raws) == 3
    end

    test "streams raw events as read from batch even in different chunks" do
      raws = MessageStore.stream(@stream_name, 1, 1) |> Enum.to_list()

      assert length(raws) == 2
    end
  end
end
