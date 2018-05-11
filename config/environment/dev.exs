use Mix.Config

config :logger,
  backends: [:console],
  utc_log: true,
  compile_time_purge_level: :error

  config :esp_ex, EspEx.Repo,
    adapter: Ecto.Adapters.Postgres,
    database: System.get_env("ESPEX_DATABASE") || "esp_ex_dev",
    username: System.get_env("ESPEX_USER") || "postgres",
    password: System.get_env("ESPEX_PASSWORD") || "postgres",
    hostname: System.get_env("ESPEX_HOSTNAME") || "localhost"
