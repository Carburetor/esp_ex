# Delugex

Evented system framework

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `delugex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delugex, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/delugex](https://hexdocs.pm/delugex).

## Index

### Key modules

- [MessageStore.Postgres](#messagestorepostgres) which can write and
  read messages from the database in the specified format. It's used mainly for
  writing, since the reading is performed transparently through `Consumer` and
  `Store`
- [EventTransformer](#eventtransformer) which allows to transform a message as
  it comes from the database into an elixir struct (an **event**) of your
  choice
- [Projection](#projection) which takes a list of events and create the
  specified entity by applying logic for each event
- [StreamName](#streamname) holds the representation of a stream name by
  converting from a raw string `"campaign:command-123"` to a struct following
  the conventions
- [Store](#store) takes a stream and an id, finds all the events on that stream
  and [projects](#projection) the requested entity, returning the final result
- [Consumer.Postgres](#consumerpostgres) listen to any incoming event and
  react by running a handler (a function which pattern matches on that event)
  - [Handler](#handler) a module with a function `handle` which pattern matches
    on the event and does any kind of work (business logic)

### Other relevant modules

- [Entity](#entity) a behaviour that should be implemented by entity modules,
  ensuring those can be initialized without any argument
- [Event.Raw](#rawevent) database representation of a _message_ in the
  _messages_ table
  - [Event.Metadata](#raweventmetadata) represent a set of useful metadata
    attributes that can be stored in a message
- [Event](#event) helpers to create a [Event.Raw](#rawevent) from a custom
  struct

## Usage

### `MessageStore.Postgres`

Assuming `alias MessageStore.Postgres, as: MessageStore` in all the following
examples:

```elixir
stream_name = Delugex.StreamName.new("person", "123")
raw = %Delugex.Event.Raw{
  type: "Created",
  data: %{name: "Some Name"}
  stream_name: stream_name
}

# Assuming the stream is empty
MessageStore.write!(raw) # => 0

# Assuming the stream has only 1 message
MessageStore.write!(raw, 0) # => 1

# Assuming the stream has 2 messages, so "version" is 1
MessageStore.write!(raw, 2) # => raises ExpectedVersionError
```

### `EventTransformer`

```elixir
defmodule Person.Events do
  use EventTransformer,
    # optional, default to this module
    events_module: __MODULE__

  defmodule Created do
    defstruct [:name]
  end
end

stream_name = Delugex.StreamName.new("person", "123")
raw = %Delugex.Event.Raw{
  type: "Created",
  data: %{name: "Some Name"}
  stream_name: stream_name
}

Person.Events.transform(raw) # => %Created{name: "Some Name"}

raw = Map.put(raw, :type, "Renamed")
Person.Events.transform(raw) # => %Delugex.Event.Unknown{...}
```

You can customize the function `transform` however you want.

### `Projection`

Assuming the modules from [EventTransformer](#eventtransformer)

```elixir
defmodule Person do
  defstruct [:name]

  def new() do
    %__MODULE__{name: "noname"}
  end
end

defmodule Person.Projection do
  use Delugex.Projection

  def apply(%Person{} = person, %Person.Events.Created{} = event) do
    Map.put(person, :name, event.name)
  end
end

person = Person.new()
created = %Person.Events.Created{name: "jerry"}

person = Person.Projection.apply(person, created)

person.name # => "jerry"
```

### `StreamName`

Creates a stream name. A stream name is in the format:
`category:type1+type2-ID`.
StreamName provides 2 very helpful constructors:

```elixir
alias Delugex.StreamName

stream_name = StreamName.new("person", "123")
to_string(stream_name) # => "person-123"

stream_name = StreamName.new("person", "123", ["position", "command"])
# The types are always sorted
to_string(stream_name) # => "person:command+position-123"

stream_name = StreamName.from_string("person:command-123")
inspect(stream_name)
# => %StreamName{category: "person", identifier: "123", types: ["command"]}
```

### `Store`

Fetches all events from a stream and project them. Assume the module

```elixir
defmodule Person.Store do
  use Delugex.Store,
    # required, an implementation of `MessageStore`
    message_store: Delugex.MessageStore.Postgres,
    # required, anything which responds to `new/0` and returns needed entity
    # initialized
    entity_builder: Person,
    # required
    event_transformer: Person.Events,
    # required
    projection: Person.Projection,
    # required, you want this to be a category stream (stream without
    # identifer)
    stream_name: Delugex.StreamName.new("person")
end

created = %Person.Events.Created{name: "jerry"}
createdAgain = %Person.Events.Created{name: "francesco"}
stream_name = Delugex.StreamName.from_string("person-123")

alias Delugex.MessageStore.Postgres, as: MessageStore

raw = Event.to_event(created, stream_name)
MessageStore.write!(raw)
raw = Event.to_event(createdAgain, stream_name)
MessageStore.write!(raw)

{person, version} = Person.Store.fetch("123")
version # => 1
person # => %Person{name: "francesco"}
# Notice how it's not "jerry", since 2 events have been applied
```

### `Consumer.Postgres`

Listen for incoming events and process them. Assume all the code in
[Store](#store)

```elixir
defmodule Person.Consumer do
  use Delugex.Consumer.Postgres,
    # required
    event_transformer: Person.Events,
    # required
    stream_name: Delugex.StreamName.new("person")
    # optional, a string uniquely identifying this consumer. Defaults to module
    # name
    identifier: __MODULE__,
    # optional, a module handling events for this module. Defaults to this
    # module
    handler: __MODULE__,
    # optional, check the documentation, you should never need this
    listen_opts: []

  use Delugex.Handler

  def handle(%Person.Events.Created{} = created, _, _) do
    IO.puts("A person was created! Hello #{created.name}")
  end
end

{:ok, consumer} = GenServer.start_link(Person.Consumer, nil)


created = %Person.Events.Created{name: "jerry"}
stream_name = Delugex.StreamName.from_string("person-123")

alias Delugex.MessageStore.Postgres, as: MessageStore

raw = Event.to_event(created, stream_name)
MessageStore.write!(raw)

# When the consumer receives the message, it will print:
# A person was created! Hello jerry
```

### `Handler`

Check [Consumer.Postgres](#consumerpostgres)

## Licensing

This code is inspired and partially reimplements [eventide](https://eventide-project.org/), as such is also subject to the [eventide LICENSE](eventide-LICENSE)
