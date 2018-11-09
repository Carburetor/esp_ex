# defmodule Delugex.Consumer.PostgresTest do
#   use ExUnit.Case

#   alias Delugex.MessageStore.Postgres, as: MessageStore
#   alias Delugex.StreamName
#   alias Delugex.Event

#   defp truncate_messages do
#     Delugex.MessageStore.Postgres.Repo
#     |> Ecto.Adapters.SQL.query!("TRUNCATE TABLE messages RESTART IDENTITY", [])
#   end

#   defmodule Events do
#     use Delugex.EventTransformer

#     defmodule Renamed do
#       defstruct []
#     end

#     defmodule Spammed do
#       defstruct []
#     end
#   end

#   defmodule CampaignConsumer do
#     use Delugex.Consumer.Postgres,
#       event_transformer: Events,
#       stream_name: StreamName.new("campaign")

#     use Delugex.Handler

#     @impl Delugex.Handler
#     def handle(%Events.Renamed{}, _, pid) do
#       send(pid, {:renamed})
#     end

#     @impl Delugex.Handler
#     def handle(%Events.Spammed{}, _, pid) do
#       send(pid, {:spammed})
#     end
#   end

#   @stream_name StreamName.new("campaign", "123")
#   @event_base %Delugex.Event.Raw{stream_name: @stream_name}

#   describe "Consumer.Postgres" do
#     test "handles renamed events" do
#       _pid = start_supervised!({CampaignConsumer, self()})

#       %Events.Renamed{}
#       |> Event.to_event(@event_base)
#       |> MessageStore.write!()

#       assert_receive {:renamed}, 500

#       truncate_messages()
#     end

#     test "handles spammed events" do
#       _pid = start_supervised!({CampaignConsumer, self()})

#       %Events.Spammed{}
#       |> Event.to_event(@event_base)
#       |> MessageStore.write!()

#       assert_receive {:spammed}, 500

#       truncate_messages()
#     end
#   end
# end
