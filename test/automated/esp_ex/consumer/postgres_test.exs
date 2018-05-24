defmodule EspEx.Consumer.PostgresTest do
  use ExUnit.Case

  alias EspEx.MessageStore.Postgres, as: MessageStore
  alias EspEx.StreamName
  alias EspEx.Event

  defp truncate_messages do
    EspEx.MessageStore.Postgres.Repo
    |> Ecto.Adapters.SQL.query!("TRUNCATE TABLE messages RESTART IDENTITY", [])
  end

  defmodule Events do
    use EspEx.EventTransformer

    defmodule Renamed do
      defstruct []
    end

    defmodule Spammed do
      defstruct []
    end
  end

  defmodule CampaignConsumer do
    use EspEx.Consumer.Postgres,
      event_transformer: Events,
      stream_name: StreamName.new("campaign")

    use EspEx.Handler

    @impl EspEx.Handler
    def handle(%Events.Renamed{}, _, pid) do
      send(pid, {:renamed})
    end

    @impl EspEx.Handler
    def handle(%Events.Spammed{}, _, pid) do
      send(pid, {:spammed})
    end
  end

  @stream_name StreamName.new("campaign", "123")
  @raw_event_base %EspEx.RawEvent{stream_name: @stream_name}

  describe "Consumer.Postgres" do
    test "handles renamed events" do
      {:ok, pid} = GenServer.start_link(CampaignConsumer, self())

      %Events.Renamed{}
      |> Event.to_raw_event(@raw_event_base)
      |> MessageStore.write!()

      assert_receive {:renamed}, 500

      GenServer.stop(pid)
      truncate_messages()
    end

    test "handles spammed events" do
      {:ok, pid} = GenServer.start_link(CampaignConsumer, self())

      %Events.Spammed{}
      |> Event.to_raw_event(@raw_event_base)
      |> MessageStore.write!()

      assert_receive {:spammed}, 500

      GenServer.stop(pid)
      truncate_messages()
    end
  end
end
