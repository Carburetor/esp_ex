defmodule Ex1.Task.Store do
  use EspEx.Store,
    entity: Ex1.Task,
    stream_category: "task",
    projection: Ex1.Task.Projection

  # To use the store
  # {:ok, task, _version} = Ex1.Task.Store.fetch(id)
end
