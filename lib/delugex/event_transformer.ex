defmodule Delugex.EventTransformer do
  @moduledoc """
  Helps converting from a raw event. A raw event is basically a map as
  it comes from the database.

  It's a behavior (fill-in the types for callbacks)

  It can be "used" with `use Delugex.EventTransformer` which would:
  - @behavior Delugex.EventTransformer
  - provide a default `transform` which catches any event and convert them (use
  the created `Delugex.EventTransformer.transform`)
  """

  alias Delugex.Event.Raw
  alias Delugex.Event.Unknown
  alias Delugex.Logger

  @callback transform(raw :: Delugex.Event.Raw.t()) ::
              any() | Delugex.Event.Unknown.t()

  defmacro __using__(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:events_module, __CALLER__.module)

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def transform(%Raw{} = raw) do
        events_module = unquote(opts)[:events_module]

        case events_module do
          nil -> Delugex.EventTransformer.transform(__MODULE__, raw)
          _ -> Delugex.EventTransformer.transform(events_module, raw)
        end
      end
    end
  end

  @doc ~S"""
  Converts from a Event.Raw to an Event, which is a struct defined
  by the user, in a module defined by the user, the only known things is that
  it has the `event_id` field and the `raw` field.

  Takes a %Raw and it creates a new Event, based on events_module plus the
  `:type` field in Event.Raw. So it becomes `#{events_module}.#{type}` (check
  for errors, create a custom struct %Delugex.Events.Unknown if it's missing).
  Then copy `event_id` to `event_id`. Then, it grabs all the remaining
  fields in Event.Raw excluding `data` and it stores it
  in `:raw` field. Finally all fields in `data` are
  copied in the Event (which is a map)
  """
  def transform(events_module, %Raw{type: type} = raw)
      when is_atom(events_module) do
    with {:ok, event_module} <- find_module(events_module, type) do
      struct(event_module, raw.data)
    else
      {:unknown, _} -> %Unknown{raw: raw}
      {:no_struct, _} -> %Unknown{raw: raw}
    end
  end

  def find_module(events_module, type)
      when is_atom(events_module) and is_binary(type) do
    try do
      event_module = Module.safe_concat([events_module, type])
      loaded = load(event_module)
      event_module_result(event_module, loaded)
    rescue
      ArgumentError ->
        Logger.warn(fn ->
          "Event #{events_module}.#{type} doesn't exist"
        end)

        {:unknown, {events_module, type}}
    end
  end

  defp load(event_module) do
    Code.ensure_loaded(event_module)
  end

  defp event_module_result(event_module, {:module, _}) do
    if function_exported?(event_module, :__struct__, 0) do
      {:ok, event_module}
    else
      Logger.warn(fn -> "Event #{event_module} has no struct" end)
      {:no_struct, event_module}
    end
  end

  defp event_module_result(event_module, error) do
    Logger.error(fn ->
      "Event #{event_module} is a not a valid module: #{inspect(error)}"
    end)

    {:invalid_module, event_module}
  end
end
