defmodule EspEx.Application do
  use Application

  def start(_type, _args) do
    repo = EspEx.MessageStore.Postgres.Repo

    children = [
      repo,
      %{
        id: Postgrex.Notifications,
        start: {
          Postgrex.Notifications,
          :start_link,
          [repo.config() ++ [name: Postgrex.Notifications]]
        }
      }
    ]

    Supervisor.start_link(
      children,
      strategy: :one_for_one,
      name: EspEx.Application
    )
  end
end
