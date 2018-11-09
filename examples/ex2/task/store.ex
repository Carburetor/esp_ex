defmodule Ex2.Task.Store do
  # To use the store
  # {:ok, task, _version} = Ex1.Task.Store.fetch(id)
  def fetch(id) do
    Delugex.Store.fetch(Ex2.Task, "task", Ex2.Task.Projection, id)
  end
end
