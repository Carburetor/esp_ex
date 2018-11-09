use Mix.Config

config :logger,
  backends: [:console],
  utc_log: true,
  compile_time_purge_level: :debug

config :delugex, Delugex.MessageStore.Postgres.Repo,
  pool_size: 15,
  url: System.get_env("DELUGEX_DATABASE_URL")
