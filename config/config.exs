# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :logger,
  backends: [:console],
  utc_log: true,
  compile_time_purge_level: :debug

config :ecto, json_library: Jason
config :esp_ex, EspEx.Repo, types: EspEx.PostgresTypes

import_config "./environment/#{Mix.env()}.exs"
