defmodule Ex1.Task.Projection do
  alias Ex1.Task.Events

  use Delugex.Projection,
    entity: Ex1.Task,
    apply_unhandled: true

  def apply(%Events.Created{} = created, entity) do
    # do something with entity and created
    Ex1.Task.created(entity, created.time)
    # return new entity
    entity
  end
end
