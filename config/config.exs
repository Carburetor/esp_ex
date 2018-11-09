# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :logger,
  backends: [:console],
  utc_log: true,
  compile_time_purge_level: :debug

config :ecto, json_library: Jason

config :esp_ex, EspEx.MessageStore.Postgres.Repo,
  adapter: Ecto.Adapters.Postgres,
  database: System.get_env("ESPEX_DATABASE") || "esp_ex_dev",
  username: System.get_env("ESPEX_USER") || "postgres",
  password: System.get_env("ESPEX_PASSWORD") || "postgres",
  hostname: System.get_env("ESPEX_HOSTNAME") || "localhost",
  types: EspEx.MessageStore.Postgres.Types

import_config "./environment/#{Mix.env()}.exs"
