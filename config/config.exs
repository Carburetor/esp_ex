# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :delugex, Delugex.MessageStore.Postgres.Repo, url: ""

config :delugex, Delugex.MessageStore.Postgres,
  stream_name: [decoder: Delugex.Stream.Name],
  json: [decoder: Jason, encoder: Jason]

import_config "./environment/#{Mix.env()}.exs"

if File.exists?(Path.expand("./environment/#{Mix.env()}.secret.exs", __DIR__)) do
  import_config "./environment/#{Mix.env()}.secret.exs"
end
