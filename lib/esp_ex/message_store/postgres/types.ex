Postgrex.Types.define(
  EspEx.MessageStore.Postgres.Types,
  [] ++ Ecto.Adapters.Postgres.extensions(),
  json: Jason
)
