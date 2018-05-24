defmodule EspEx.Consumer.Config do
  @moduledoc """
  Consumer configuration representation
  """

  @type t :: %EspEx.Consumer.Config{
          event_bus: module(),
          event_transformer: module(),
          stream_name: EspEx.StreamName.t(),
          identifier: String.t(),
          handler: module(),
          listen_opts: EspEx.EventBus.listen_opts()
        }
  defstruct [
    :event_bus,
    :event_transformer,
    :stream_name,
    :identifier,
    :handler,
    :listen_opts
  ]

  @doc """
  - `:event_bus` **required** an `EspEx.EventBus` implementation
  - `:event_transformer` **required** an `EspEx.EventTransformer`
    implementation
  - `:stream_name` **required** a `EspEx.StreamName`
  - `:identifier` (optional) a `String` identifying uniquely this consumer
  - `:handler` (optional) a `EspEx.Handler` implementation
  """
  def new(opts) when is_list(opts) do
    %__MODULE__{
      event_bus: Keyword.get(opts, :event_bus),
      event_transformer: Keyword.get(opts, :event_transformer),
      stream_name: Keyword.get(opts, :stream_name),
      identifier: Keyword.get(opts, :identifier),
      handler: Keyword.get(opts, :handler),
      listen_opts: Keyword.get(opts, :listen_opts, [])
    }
  end
end
