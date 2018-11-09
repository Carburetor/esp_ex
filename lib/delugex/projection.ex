defmodule Delugex.Projection do
  @moduledoc """
  Project events upon an entity to convert it to an up-to-date value.

  ## use Delugex.Projection

  Will provide a default `apply` implementation that will catch any event and
  just return the entity as is. It will also log a warn, reporting that the
  event is unhandled.

  The developer is expected to `use` this module and provide its own
  implementations of `apply` (using guard-clauses).

  ### Examples

  ```
  defmodule UserProjection do
    use Delugex.Projection

    def apply(%User{} = user, %EmailChanged{email: email}) do
      Map.put(user, :email, email)
    end
  end

  In this case, when called with `apply(%User{}, %NotHandledEvent{})` the
  developer will simply get back `%User{}`, however if called with:

  ```
  apply(%User{email: "foo@bar.com"}, %EmailChanged{email: "test@example.com"}))
  ```

  The returned value is `%User{email: "test@example.com"}`
  """

  alias Delugex.Logger

  @callback apply(entity :: any(), event :: any()) :: any()
  @callback apply_all(
              entity :: any(),
              events :: Enumerable.t()
            ) :: any()

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)
      @before_compile Delugex.Projection.Unhandled

      @impl unquote(__MODULE__)
      def apply_all(entity, events) when is_list(events) do
        unquote(__MODULE__).apply_all(__MODULE__, entity, events)
      end
    end
  end

  @doc """
  Takes an entity, a list of events and a module that can project such events
  on the passed entity and it applies all of them, returning the newly updated
  entity.
  It also logs (debug) info whenever an event is applied
  """
  @spec apply_all(
          projection :: module,
          current_entity :: any(),
          events :: Enumerable.t()
        ) :: any()
  def apply_all(projection, current_entity, events \\ []) do
    Enum.reduce(events, current_entity, fn event, entity ->
      Logger.debug(fn ->
        "Applying #{event.__struct__}"
      end)

      projection.apply(entity, event)
    end)
  end
end
