use Mix.Config

config :logger,
  backends: [],
  utc_log: true,
  compile_time_purge_level: :debug

config :delugex, Delugex.MessageStore.Postgres, notify: false

config :delugex, Delugex.MessageStore.Postgres.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  url:
    System.get_env("TEST_DELUGEX_DATABASE_URL") ||
      "postgres://message_store:message_store@localhost/delugex_test?pool_size=15"
