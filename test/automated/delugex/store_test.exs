defmodule Delugex.StoreTest do
  use Delugex.Case

  alias Delugex.MessageStore.Static, as: MessageStore
  alias Delugex.Stream.Name

  defmodule Person do
    defstruct [:name]

    def new, do: %__MODULE__{name: "<unnamed>"}
  end

  defmodule Events do
    use Delugex.EventTransformer

    defmodule Updated do
      defstruct [:name]
    end

    defmodule Nopped do
      defstruct []
    end
  end

  defmodule Projection do
    use Delugex.Projection

    @impl Delugex.Projection
    def apply(nil, event), do: __MODULE__.apply(%Person{}, event)

    @impl Delugex.Projection
    def apply(%Person{} = person, %Events.Updated{name: name}) do
      Map.put(person, :name, name)
    end
  end

  defmodule Store do
    use Delugex.Store,
      message_store: MessageStore,
      entity_builder: Person,
      event_transformer: Events,
      projection: Projection,
      stream_name: %Name{
        category: "campaign",
        id: "123"
      }
  end

  describe "Store.fetch" do
    test "creates a new entity and updates it to the last version" do
      {entity, _} = Store.fetch()

      assert entity.name == "Lol"
    end

    test "returns the last event position" do
      {_, position} = Store.fetch("123")

      assert position == 2
    end

    test "returns nil position when channel is empty" do
      {_, position} = Store.fetch("foo453")

      assert position == nil
    end

    test "returns nil entity when channel is empty" do
      {entity, _} = Store.fetch("foo453")

      assert entity == nil
    end
  end
end
