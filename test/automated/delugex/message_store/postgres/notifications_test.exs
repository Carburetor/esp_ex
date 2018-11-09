# defmodule Delugex.MessageStore.Postgres.NotificationsTest do
#   use ExUnit.Case

#   alias Delugex.StreamName
#   alias Delugex.MessageStore.Postgres

#   @stream_name %StreamName{category: "campaign", identifier: "123", types: []}
#   @text_stream to_string(@stream_name)

#   # If you run theses tests very fast (`mix test` twice in a row), they might
#   # fail due to postgres.
#   # Please wait ~10 seconds and re-run `mix test`. Postgres can decide to
#   # ignore the exact same notification repeated twice in a short timespan

#   describe "Notifications.listen" do
#     test "starts receiving database notifications" do
#       {:ok, ref} = Postgres.listen(@stream_name)
#       Postgres.notify(@text_stream, "bar")

#       assert_receive {:notification, _, _, @text_stream, "bar"}

#       Postgres.unlisten(ref)
#     end

#     test "starts receiving timer notifications" do
#       {:ok, ref} = Postgres.listen(@stream_name, interval: 100)

#       assert_receive {:reminder}, 500

#       Postgres.unlisten(ref)
#     end
#   end

#   describe "Notifications.unlisten" do
#     test "cancels any listening process, stopping messages from postgres" do
#       {:ok, ref} = Postgres.listen(@stream_name)
#       Postgres.unlisten(ref)
#       Postgres.notify(@text_stream, "never_sent")

#       refute_receive {:notification, _, _, @text_stream, "never_sent"}
#     end

#     test "cancels any listening process, stopping messages from timer" do
#       {:ok, ref} = Postgres.listen(@stream_name, interval: 50)
#       Postgres.unlisten(ref)

#       refute_receive {:reminder}
#     end
#   end

#   describe "Notifications.notify" do
#     test "passes data with the specified notification" do
#       {:ok, ref} = Postgres.listen(@stream_name)
#       Postgres.notify(@text_stream, "foo")

#       assert_receive {:notification, _, _, @text_stream, "foo"}, 500

#       Postgres.unlisten(ref)
#     end
#   end
# end
