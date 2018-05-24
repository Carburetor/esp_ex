defmodule EspEx.MessageStore do
  @moduledoc """
  This module provides an interface for specialized event buses. In addition,
  it provides a utility macro that auto-implements write_initial! for you based
  on `write!` implementation (expects that no messages are present on stream)
  """

  @type listen_ref :: any
  @type listen_opts :: list()
  @type expected_version :: non_neg_integer | :no_stream | nil
  @type batch_size :: non_neg_integer
  @type version :: non_neg_integer

  alias EspEx.MessageStore.Stream.Position

  # Raises `EspEx.MessageStore.ExpectedVersionError` in case of version violation
  @callback write!(
              raw_event :: EspEx.RawEvent.t(),
              expected_version :: expected_version()
            ) :: version()
  # Raises `EspEx.MessageStore.ExpectedVersionError` in case of version violation
  @callback write_initial!(raw_event :: EspEx.RawEvent.t()) :: no_return()
  @callback read_last(stream_name :: EspEx.StreamName.t()) ::
              EspEx.RawEvent.t() | nil
  @callback read_batch(
              stream_name :: EspEx.StreamName.t(),
              position :: version(),
              batch_size :: batch_size()
            ) :: list(EspEx.RawEvent.t())
  @callback read_version(stream_name :: EspEx.StreamName.t()) :: version() | nil
  @callback stream(
              stream_name :: EspEx.StreamName.t(),
              position :: version(),
              batch_size :: batch_size()
            ) :: Enumerable.t()
  @callback listen(
              stream_name :: EspEx.StreamName.t(),
              opts :: listen_opts()
            ) :: {:ok, listen_ref()} | {:error, any}
  @callback unlisten(
              ref :: listen_ref(),
              opts :: listen_opts()
            ) :: {:ok, listen_ref()} | {:error, any}

  defguard is_version(version) when is_integer(version) and version >= 0

  defguard is_expected_version(version)
           when version == :no_stream or is_nil(version) or is_version(version)

  defguard is_batch_size(size) when is_integer(size) and size >= 0

  @spec write_initial!(
          message_store :: module,
          raw_event :: EspEx.RawEvent.t()
        ) :: no_return()
  def write_initial!(message_store, raw_event) do
    message_store.write!(raw_event, :no_stream)
  end

  @spec stream(
          message_store :: module,
          stream_name :: EspEx.StreamName.t(),
          position :: version(),
          batch_size :: batch_size()
        ) :: Enumerable.t()
  def stream(
        message_store,
        %EspEx.StreamName{} = stream_name,
        position \\ 0,
        batch_size \\ 10
      )
      when is_version(position) and is_batch_size(batch_size) do
    stream_pos = Position.new(message_store, stream_name, position, batch_size)
    EspEx.MessageStore.Stream.from_position(stream_pos)
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def write_initial!(raw_event) do
        EspEx.MessageStore.write_initial!(__MODULE__, raw_event)
      end

      @impl unquote(__MODULE__)
      def stream(stream_name, position \\ 0, batch_size \\ 10) do
        EspEx.MessageStore.stream(__MODULE__, stream_name, position, batch_size)
      end

      defoverridable write_initial!: 1, stream: 3
    end
  end
end
