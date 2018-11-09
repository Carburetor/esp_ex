defmodule User.Events do
  use Delugex.EventTransformer

  defmodule Created do
    defstruct [:name]
  end

  defmodule Renamed do
    defstruct [:name]
  end
end

defmodule User do
  defstruct [:name]
end

defmodule User.Projection do
  use Delugex.Projection

  alias User.Events

  def apply(nil, event), do: __MODULE__.apply(%User{name: nil}, event)

  def apply(user, %Events.Created{name: name}) do
    IO.inspect(user)

    case user.name do
      nil -> Map.put(user, :name, name)
      _ -> user
    end
  end

  def apply(user, %Events.Renamed{name: name}) do
    Map.put(user, :name, name)
  end
end

defmodule User.Store do
  use Delugex.Store,
    message_store: Delugex.MessageStore.Mnesia,
    event_transformer: User.Events,
    projection: User.Projection,
    stream_name: Delugex.Stream.Name.new("user")
end

defmodule User.Consumer do
  use Delugex.Handler

  use Delugex.Consumer.Mnesia,
    event_transformer: User.Events,
    stream_name: Delugex.Stream.Name.new("user")

  alias User.Events
  require Logger

  def handle(%Events.Created{}, %{stream_name: %{id: id}}, _meta) do
    {user, _} = User.Store.fetch(id)
    Logger.error("created #{inspect(user)}")
  end

  def handle(%Events.Renamed{}, _, _meta) do
    Logger.error("created")
  end
end

defmodule ManualTest do
  def run do
    :mnesia.start()
    Delugex.MessageStore.Mnesia.start()

    case User.Store.fetch("123") do
      {_, nil} ->
        %User.Events.Created{name: "Francesco"}
        |> Delugex.Event.to_event(Delugex.Stream.Name.new("user", "123"))
        |> Delugex.MessageStore.Mnesia.write!()

      _ ->
        nil
    end

    User.Consumer.start_link()

    %User.Events.Created{name: "Francesco"}
    |> Delugex.Event.to_event(Delugex.Stream.Name.new("user", "123"))
    |> Delugex.MessageStore.Mnesia.write!()
  end
end

ManualTest.run()
