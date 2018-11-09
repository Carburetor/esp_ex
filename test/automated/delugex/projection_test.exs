defmodule Delugex.ProjectionTest do
  use Delugex.Case
  alias Delugex.Projection

  defmodule Person do
    defstruct [:name]
  end

  defmodule Nopped do
    defstruct []
  end

  defmodule Renamed do
    defstruct [:name]
  end

  defmodule PersonProjection do
    use Delugex.Projection

    @impl Delugex.Projection
    def apply(%Person{} = person, %Renamed{name: name}) do
      Map.put(person, :name, name)
    end
  end

  describe "Projection.apply" do
    test "defaults to returning passed entity" do
      entity = PersonProjection.apply(%Person{name: "blah"}, %Nopped{})

      assert entity == %Person{name: "blah"}
    end

    test "is used only for non-overridden clauses" do
      entity =
        %Person{name: "blah"}
        |> PersonProjection.apply(%Nopped{})
        |> PersonProjection.apply(%Renamed{name: "jerry"})

      assert entity == %Person{name: "jerry"}
    end
  end

  describe "Projection.apply_all" do
    test "applies list of events to entity using given projection" do
      events = [%Nopped{}, %Renamed{name: "jerry"}, %Renamed{name: "tom"}]

      entity = %Person{name: "blah"}
      entity = Projection.apply_all(PersonProjection, entity, events)

      assert entity == %Person{name: "tom"}
    end
  end
end
