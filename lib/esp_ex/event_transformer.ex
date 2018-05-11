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

  @callback to_event(raw_event)
  @callback to_raw_event(event)

  @doc """
  Converts from a RawEvent to an Event, which is a struct defined
  by the user, in a module defined by the user, the only known things is that
  it has the `id` field and the `raw_metadata` field.

  Takes a %RawEvent and it creates a new Event, based on events_module plus the
  `:type` field in RawEvent. So it becomes `#{events_module}.#{type}` (check
  for errors, create a custom struct %EspEx.Events.Unknown if it's missing).
  Then copy `id` to `id`. Then, it grabs all the remaining
  fields in RawEvent excluding `data` and it creates a `RawMetadata` out of it,
  which is stored in `:raw_metadata` field. Finally all fields in `data` are
  copied in the Event (which is a map)
  """
  def to_event(events_module, raw_event) do
  end

  @doc """
  Converts from a user defined Event to a RawEvent. It copies `id` to `id`,
  then everything in `raw_metadata` becomes normal fields in RawEvent. Finally,
  any field remaining in `Event` (after removing :id and :raw_metadata) goes
  into Event `data` field

  Takes a raw event (basically a map of the row coming from the database) and
  converts it to a user-defined struct (so that the user can pattern-match).
  For example:
  %RawEvent{id: "123", type: "Created"}
  """
  def to_raw_event(event) do
  end
end
