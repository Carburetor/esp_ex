defmodule EspEx.EventTransformer do
  @moduledoc """
  Helps converting from and to a raw event. A raw event is basically a map as
  it comes from the database.

  It's a behavior (fill-in the types for callbacks)

  It can be "used" with `use EspEx.EventTransformer` which would:
  - @behavior EspEx.EventTransformer
  - provide a default `to_event` which catches any event and convert them (use
  the created `EspEx.EventTransformer.to_event`)
  """

  alias EspEx.RawEvent
  alias EspEx.Event.Unknown
  alias EspEx.Logger

  @callback to_event(raw_event :: EspEx.RawEvent.t()) ::
              struct | EspEx.Event.Unknown.t()

  defmacro __using__(opts \\ []) do
    events_module = Keyword.get(opts, :events_module, __CALLER__.module)

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def to_event(%RawEvent{} = raw_event) do
        events_module = unquote(events_module)

        case events_module do
          nil -> EspEx.EventTransformer.to_event(__MODULE__, raw_event)
          _ -> EspEx.EventTransformer.to_event(events_module, raw_event)
        end
      end
    end
  end

  @doc ~S"""
  Converts from a RawEvent to an Event, which is a struct defined
  by the user, in a module defined by the user, the only known things is that
  it has the `event_id` field and the `raw_event` field.

  Takes a %RawEvent and it creates a new Event, based on events_module plus the
  `:type` field in RawEvent. So it becomes `#{events_module}.#{type}` (check
  for errors, create a custom struct %EspEx.Events.Unknown if it's missing).
  Then copy `event_id` to `event_id`. Then, it grabs all the remaining
  fields in RawEvent excluding `data` and it stores it
  in `:raw_event` field. Finally all fields in `data` are
  copied in the Event (which is a map)
  """
  def to_event(events_module, %RawEvent{type: type} = raw_event)
      when is_atom(events_module) do
    with {:ok, event_module} <- to_event_module(events_module, type) do
      struct(event_module, raw_event.data)
    else
      {:unknown, _} -> %Unknown{raw_event: raw_event}
      {:no_struct, _} -> %Unknown{raw_event: raw_event}
    end
  end

  def to_event_module(events_module, type)
      when is_atom(events_module) and is_bitstring(type) do
    try do
      event_module = Module.safe_concat([events_module, type])
      load_and_to_result(event_module)
    rescue
      ArgumentError ->
        Logger.warn(fn ->
          "Event #{events_module}.#{type} doesn't exist"
        end)

        {:unknown, {events_module, type}}
    end
  end

  defp load_and_to_result(event_module) do
    loaded = Code.ensure_loaded(event_module)
    event_module_result(event_module, loaded)
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
