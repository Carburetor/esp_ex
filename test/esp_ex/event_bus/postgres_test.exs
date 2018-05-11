defmodule EspEx.EventBus.PostgresTest do
  use ExUnit.Case #, async: true
  alias EspEx.EventBus.Postgres

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(EspEx.Repo)
    Ecto.Adapters.SQL.Sandbox.mode(EspEx.Repo, {:shared, self()})
  end

  # TODO write tests when finalized
  #test "write & read" do
  #  guid = UUID.uuid4()
  #  result = Postgres.write(
  #    UUID.uuid4(), "cool_beans", "type", %{some: :data}
  #  )

  #  IO.inspect result, label: "ZZZZZZZZZZ"
  #  IO.inspect Postgres.get_batch("cool_beans")
  #end

  test "postrex" do
    #pg_config = Application.get_env(:esp_ex, EspEx.Repo)
    #IO.inspect pg_config
    #{:ok, pid} = Postgrex.Notifications.start_link(pg_config)
    #IO.inspect Postgrex.Notifications.listen(pid, "dave")

    Postgres.listen("dave")
    db_config = [hostname: "localhost", username: "postgres", password: "postgres",
                 database: "esp_ex_test"]
    # Postgres.database_config
    #

    {:ok, pid} = Postgrex.start_link(db_config)
    IO.inspect Process.whereis(EspEx.Repo)
    #Postgrex.query!(pid, "notify dave, 'dave'", [])

    Ecto.Adapters.SQL.query(EspEx.Repo, "notify dave, 'cool'", [])

    #:timer.sleep(1000)
    Postgres.notify("dave", "something")

    receive do
      notification -> 
        IO.inspect notification
    end

    #IO.inspect(pid)
  end

end
