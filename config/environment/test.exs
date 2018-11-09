use Mix.Config

config :logger,
  backends: [],
  utc_log: true,
  compile_time_purge_level: :debug

config :esp_ex, EspEx.MessageStore.Postgres.Repo,
  database: System.get_env("ESPEX_DATABASE") || "esp_ex_test",
  username: System.get_env("ESPEX_USER") || "postgres",
  password: System.get_env("ESPEX_PASSWORD") || "postgres",
  hostname: System.get_env("ESPEX_HOSTNAME") || "localhost",
  pool: Ecto.Adapters.SQL.Sandbox
