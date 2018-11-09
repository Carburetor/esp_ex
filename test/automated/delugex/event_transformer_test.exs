defmodule Delugex.EventTransformerTest do
  use Delugex.Case

  defmodule Events do
    use Delugex.EventTransformer

    defmodule Created do
      defstruct [:user_id, :email, :name, :birthdate]
    end

    defmodule Updated do
      defstruct [:user_id, :email, :name, :birthdate]
    end
  end

  defmodule Transformer do
    use Delugex.EventTransformer,
      events_module: Delugex.EventTransformerTest.Events
  end

  defp raw(type) do
    %Delugex.Event.Raw{
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
    raw_ev = raw("Created")

    %Events.Created{
      user_id: raw_ev.data.user_id,
      email: raw_ev.data.email,
      name: raw_ev.data.name,
      birthdate: raw_ev.data.birthdate
    }
  end

  def unknown_event do
    raw_ev = raw("foo")
    %Delugex.Event.Unknown{raw: raw_ev}
  end

  describe "EventTransformer.transform" do
    test "transforms Event.Raw into a user defined event (Created)" do
      raw_ev = raw("Created")
      created_ev = created_event()
      event = Events.transform(raw_ev)

      assert event == created_ev
    end

    test "transforms Event.Raw into an Event.Unknown when type is unknown" do
      raw_ev = raw("foo")
      unknown_ev = unknown_event()
      event = Events.transform(raw_ev)

      assert event == unknown_ev
    end

    test "transforms Event.Raw into a user event using external module" do
      raw_ev = raw("Created")
      created_ev = created_event()
      event = Transformer.transform(raw_ev)

      assert event == created_ev
    end
  end
end
