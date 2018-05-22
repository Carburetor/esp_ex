defmodule EspEx.EventTransformerTest do
  use ExUnit.Case, async: true

  defmodule Events do
    use EspEx.EventTransformer

    defmodule Created do
      defstruct [:user_id, :email, :name, :birthdate]
    end

    defmodule Updated do
      defstruct [:user_id, :email, :name, :birthdate]
    end
  end

  defmodule Transformer do
    use EspEx.EventTransformer, events_module: EspEx.EventTransformerTest.Events
  end

  defp raw_event(type) do
    %EspEx.RawEvent{
      id: 123_456,
      stream_name: "user-999",
      type: type,
      position: 100,
      global_position: 150,
      data: %{
        user_id: 999,
        email: "johndoe@mail.com",
        name: "John Doe",
        birthdate: "1991-05-01"
      },
      time: NaiveDateTime.from_iso8601!("2018-05-01T17:00:55.135053")
    }
  end

  defp created_event do
    raw_ev = raw_event("Created")

    %Events.Created{
      user_id: raw_ev.data.user_id,
      email: raw_ev.data.email,
      name: raw_ev.data.name,
      birthdate: raw_ev.data.birthdate
    }
  end

  def unknown_event do
    raw_ev = raw_event("foo")
    %EspEx.Event.Unknown{raw_event: raw_ev}
  end

  describe "EventTransformer.to_event" do
    test "transforms RawEvent into a user defined event (Created)" do
      raw_ev = raw_event("Created")
      created_ev = created_event()
      event = Events.to_event(raw_ev)

      assert event == created_ev
    end

    test "transforms RawEvent into an Event.Unknown when type is unknown" do
      raw_ev = raw_event("foo")
      unknown_ev = unknown_event()
      event = Events.to_event(raw_ev)

      assert event == unknown_ev
    end

    test "transforms RawEvent into a user event using external module" do
      raw_ev = raw_event("Created")
      created_ev = created_event()
      event = Transformer.to_event(raw_ev)

      assert event == created_ev
    end
  end
end
