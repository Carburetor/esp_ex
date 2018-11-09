# Modules

- Delugex.StreamName
  - defstruct [:category (string), :identifier (string), :types (ordset)]
  - new (args):
    - category,
    - optional identifier (nil default),
    - optional types (empty list, gets converted to ordset from a list)
  - from_string(string) returns %StreamName
  - to_string(%StreamName)
  - has_all_types checks if a list is a sublist of stuct types
  - category?
  - position_identifier(%StreamName, uint) returns string ("foo-123/1")
- Delugex.EventBus
  - @callback write
  - @callback write_initial
  - @callback read_last
  - @callback read_batch
  - @callback read_version
  - write_initial (write with expected_version nil)
  - use macro that adds write_initial to the module (e.g. Postgres) and
    @behavior Delugex.EventBus
  - Delugex.EventBus.Postgres
    - use Delugex.EventBus
    - write
    - read_last
    - read_batch
    - listen
    - unlisten
    - read_version
- Delugex.Entity
  - @callback new (no args)
- Delugex.Projection
  - @callback apply
  - use macro (creates an `apply` catch-all, adds @behavior Delugex.Projection)
  - apply_all(projection module with @behavior, entity, event list)
- Delugex.Event.Raw
  - defstruct [:id, :stream_name, :type, :position, :global_position, :data,
    :metadata, :time]
- Delugex.Event.RawMetadata
  - defstruct [:stream_name, :position, :global_position, :metadata, :time]
- Delugex.Event
  - caused_by(event) sets RawMetadata metadata field following
    [eventide specs](https://github.com/eventide-project/messaging/blob/6027504b4b505a233f74d055321c262a61003803/lib/messaging/message/metadata.rb)
  - use macro that provides macro `defevent`
    - defevent is just defstruct + [:id, :raw_metadata]
  - Delugex.Event.Causation (protocol)
    - caused_by
- Delugex.EventTransformer
  - Check docs in the EventTransformer.ex file
- Delugex.Store
  - fetch(EventBus module (Postgres), Entity module, Projection module,
    %StreamName) returns {:ok, entity, version}
  - @callback fetch(id) returns {:ok, entity, version}
  - use macro that adds @behavior and `fetch` function which accepts
    identifier (used for StreamName)
- Delugex.Handler
  - @callback handle(event)
  - use macro that provides automatically `handle` catch-all
- Delugex.Consumer
  - start_link(module, options)
    - start_polling
    - repeatedly call handle_call({:consume_event}) as long as there is
      something in buffer
  - start_polling(module, interval)
    - Start a timer, every X seconds fires :request_event and restart the timer
  - use macro
  - handle_call({:consume_event}) -> handler.handle, then
    record current position (based on identifier), then
    handle_call({:consume_event}) if there are still messages in the buffer
  - handle_cast({:request_event}) -> get_batch or use events from the existing
    buffer, fill in buffer. If there is anything in the buffer, run
    handle_call({:consume_event})
  - Delugex.Consumer.Postgres
    - use macro which uses Delugex.Consumer
    - listen
      - run handle_cast({:request_event})
    - unlisten
      - Stops listen
- Delugex.Logger
  - Same functions as https://hexdocs.pm/logger/Logger.html , just wrapping it

## Optimizations

- Event dispatcher
- Position Store
- Snapshotting
