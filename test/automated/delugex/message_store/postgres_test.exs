defmodule Delugex.MessageStore.PostgresTest do
  use Delugex.Case, async: false
  alias Delugex.MessageStore.Postgres
  alias Delugex.Stream.Name
  alias Delugex.Event

  @stream_name %Name{category: "campaign", id: "123"}
  @raw %Event{
    id: Ecto.UUID.generate(),
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Unnamed"}
  }
  @raw2 %Event{
    id: Ecto.UUID.generate(),
    stream_name: @stream_name,
    type: "Updated",
    data: %{name: "Jerry"}
  }

  setup do
    start_supervised(Delugex.MessageStore.Postgres)
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Delugex.MessageStore.Postgres.Repo)
  end

  describe "Postgres.write!" do
    test "writes raw and returns version" do
      version = Postgres.write!(@raw)

      assert version == 0
    end

    test "raises when expected version differs from actual" do
      assert_raise Delugex.MessageStore.ExpectedVersionError, fn ->
        Postgres.write!(@raw, 30)
      end
    end
  end

  describe "Postgres.write_batch!" do
    test "writes raws and returns version" do
      version =
        Postgres.write_batch!(
          [@raw, @raw2],
          @stream_name,
          :no_stream
        )

      assert version == 1
    end

    test "raises when expected version differs from actual" do
      assert_raise Delugex.MessageStore.ExpectedVersionError, fn ->
        Postgres.write_batch!([@raw, @raw2], @stream_name, 30)
      end
    end
  end

  describe "Postgres.read_last" do
    test "reads last event" do
      Postgres.write!(@raw)
      Postgres.write!(@raw2)
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
      Postgres.write!(@raw)
      Postgres.write!(@raw2)
      events = Postgres.read_batch(@stream_name)
      data = List.last(events).data

      assert data.name == "Jerry"
    end

    test "reads a list of events" do
      Postgres.write!(@raw)
      Postgres.write!(@raw2)
      events = Postgres.read_batch(@stream_name)

      assert length(events) == 2
    end
  end
end
