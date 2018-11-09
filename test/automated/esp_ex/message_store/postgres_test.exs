defmodule EspEx.MessageStore.PostgresTest do
  use ExUnit.Case, async: true
  alias EspEx.MessageStore.Postgres
  alias EspEx.StreamName
  alias EspEx.RawEvent

  @stream_name %StreamName{category: "campaign", identifier: "123", types: []}
  @raw_event %RawEvent{
    id: UUID.uuid4(),
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Unnamed"}
  }
  @raw_event2 %RawEvent{
    id: UUID.uuid4(),
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Jerry"}
  }

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(EspEx.MessageStore.Postgres.Repo)
  end

  describe "Postgres.write!" do
    test "writes raw_event and returns version" do
      version = Postgres.write!(@raw_event)

      assert version == 0
    end

    test "raises when expected version differs from actual" do
      assert_raise EspEx.MessageStore.ExpectedVersionError, fn ->
        Postgres.write!(@raw_event, 30)
      end
    end
  end

  describe "Postgres.write_batch!" do
    test "writes raw_events and returns version" do
      version =
        Postgres.write_batch!(
          [@raw_event, @raw_event2],
          @stream_name,
          :no_stream
        )

      assert version == 1
    end

    test "raises when nothing supplied" do
      assert_raise EspEx.MessageStore.EmptyBatchError, fn ->
        Postgres.write_batch!([], @stream_name)
      end
    end

    test "raises when expected version differs from actual" do
      assert_raise EspEx.MessageStore.ExpectedVersionError, fn ->
        Postgres.write_batch!([@raw_event, @raw_event2], @stream_name, 30)
      end
    end
  end

  describe "Postgres.read_last" do
    test "reads last event" do
      Postgres.write!(@raw_event)
      Postgres.write!(@raw_event2)
      event = Postgres.read_last(@stream_name)
      data = event.data

      assert data.name == "Jerry"
    end

    test "returns nil when no event found" do
      event = Postgres.read_last(@stream_name)

      assert event == nil
    end
  end

  describe "Postgres.read_batch" do
    test "reads events in order" do
      Postgres.write!(@raw_event)
      Postgres.write!(@raw_event2)
      events = Postgres.read_batch(@stream_name)
      data = List.last(events).data

      assert data.name == "Jerry"
    end

    test "reads a list of events" do
      Postgres.write!(@raw_event)
      Postgres.write!(@raw_event2)
      events = Postgres.read_batch(@stream_name)

      assert length(events) == 2
    end
  end
end
