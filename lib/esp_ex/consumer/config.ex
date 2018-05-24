defmodule EspEx.Consumer.Config do
  @moduledoc """
  Consumer configuration representation
  """

  @type t :: %EspEx.Consumer.Config{
          message_store: module(),
          event_transformer: module(),
          stream_name: EspEx.StreamName.t(),
          identifier: String.t(),
          handler: module(),
          listen_opts: EspEx.MessageStore.listen_opts()
        }
  defstruct [
    :message_store,
    :event_transformer,
    :stream_name,
    :identifier,
    :handler,
    :listen_opts
  ]

  @doc """
  - `:message_store` **required** an `EspEx.MessageStore` implementation
  - `:event_transformer` **required** an `EspEx.EventTransformer`
    implementation
  - `:stream_name` **required** a `EspEx.StreamName`
  - `:identifier` (optional) a `String` identifying uniquely this consumer
  - `:handler` (optional) a `EspEx.Handler` implementation
  """
  def new(opts) when is_list(opts) do
    %__MODULE__{
      message_store: Keyword.get(opts, :message_store),
      event_transformer: Keyword.get(opts, :event_transformer),
      stream_name: Keyword.get(opts, :stream_name),
      identifier: Keyword.get(opts, :identifier),
      handler: Keyword.get(opts, :handler),
      listen_opts: Keyword.get(opts, :listen_opts, [])
    }
  end
end
