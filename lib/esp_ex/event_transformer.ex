defmodule EspEx.EventTransformer do
  @moduledoc """
  Helps converting from and to a raw event. A raw event is basically a map as
  it comes from the database.

  It's a behavior (fill-in the types for callbacks)

  It can be "used" with `use EspEx.EventTransformer` which would:
  - @behavior EspEx.EventTransformer
  - provide a default `to_event` which catches any event and convert them (use
    the created `EspEx.EventTransformer.to_event`)
  - provide a default `to_raw_event`
  """

  @callback to_event(module, EspEx.RawEvent.t()) :: struct | EspEx.UnknownEvent.t()
  @callback to_raw_event(struct) :: EspEx.RawEvent.t()

  def base_event_fields, do: [:event_id, :raw_event]

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)

      @spec to_event(module, EspEx.RawEvent.t()) :: struct | EspEx.UnknownEvent.t()
      @spec to_raw_event(struct) :: EspEx.RawEvent.t()

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
      @impl EspEx.EventTransformer
      def to_event(events_module, raw_event) do
        type = String.capitalize(raw_event.type)
        string_module = to_string(events_module)
        modules = [string_module, type]
        event_module = safe_concat(modules)

        build_event(event_module, raw_event)
      end

      defp build_event(event_module = EspEx.UnknownEvent, raw_event) do
        struct(EspEx.UnknownEvent, Map.from_struct(raw_event))
      end

      defp build_event(event_module, raw_event) do
        event = struct(event_module, raw_event.data)
        raw_event = Map.put(raw_event, :data, nil)

        event
        |> Map.put(:event_id, raw_event.event_id)
        |> Map.put(:raw_event, raw_event)
      end

      @doc """
      Converts from a user defined Event to a RawEvent. It copies `event_id` to
      `event_id`, then everything in `raw_event` becomes normal fields in
      RawEvent. Finally, any field remaining in `Event` (after removing :event_id and
      :raw_event) goes into Event `data` field

      Takes a raw event (basically a map of the row coming from the database) and
      converts it to a user-defined struct (so that the user can pattern-match).
      For example:
      %RawEvent{event_id: "123", type: "Created"}
      """
      @impl EspEx.EventTransformer
      def to_raw_event(event) do
        type = determine_type(event)

        raw_event =
          event.raw_event
          |> Map.put(:event_id, event.event_id)
          |> Map.put(:type, type)

        Map.put(raw_event, :data, extract_data(raw_event, event))
      end

      defp safe_concat(modules) do
        try do
          # TODO add more safety
          # Code.ensure_compiled?(event)
          # function_exported?(event, :__struct__, 0)
          Module.safe_concat(modules)
        rescue
          # TODO log?
          ArgumentError ->
            EspEx.UnknownEvent
        end
      end

      defp determine_type(event) do
        event.__struct__
        |> to_string()
        |> String.split(".")
        |> List.last()
        |> String.downcase()
      end

      # TODO there's probably a more readable way to do this.
      defp extract_data(raw_event, event) do
        # data == the rest of the key/vals (set difference)
        event_keys = Map.keys(event)
        # excluding raw_ev
        raw_event_keys = Map.keys(raw_event) ++ [:raw_event]

        event_keys_set = MapSet.new(event_keys)
        raw_event_keys_set = MapSet.new(raw_event_keys)
        diff_keys = MapSet.difference(event_keys_set, raw_event_keys_set)
        Map.take(event, diff_keys)
      end
    end
  end
end
