defmodule Delugex.MessageStore do
  @moduledoc """
  This module provides an interface for specialized event buses. In addition,
  it provides a utility macro that auto-implements write_initial! for you based
  on `write!` implementation (expects that no messages are present on stream)
  """

  @type listen_ref :: any
  @type listen_opts :: list()
  @type batch_size :: non_neg_integer
  @type version :: non_neg_integer
  @type optional_version :: version() | nil
  @type expected_version :: optional_version() | :no_stream
  @type stream_version :: version() | :no_stream

  alias Delugex.MessageStore.Stream.Position

  # Raises `Delugex.MessageStore.ExpectedVersionError` in case of version violation
  @callback write!(
              event :: Delugex.Event.t(),
              expected_version :: expected_version()
            ) :: version()
  # Raises `Delugex.MessageStore.ExpectedVersionError` in case of version violation
  @callback write_initial!(event :: Delugex.Event.t()) :: no_return()
  # Raises `Delugex.MessageStore.ExpectedVersionError` in case of version violation
  # nil is returned if events is an empty list
  @callback write_batch!(
              events :: list(Delugex.Event.t()),
              stream_name :: Delugex.StreamName.t(),
              expected_version :: expected_version()
            ) :: optional_version()
  @callback read_last(stream_name :: Delugex.StreamName.t()) ::
              Delugex.Event.Raw.t() | nil
  @callback read_batch(
              stream_name :: Delugex.StreamName.t(),
              position :: version(),
              batch_size :: batch_size()
            ) :: list(Delugex.Event.Raw.t())
  @callback read_version(stream_name :: Delugex.StreamName.t()) ::
              stream_version()
  @callback stream(
              stream_name :: Delugex.StreamName.t(),
              position :: version(),
              batch_size :: batch_size()
            ) :: Enumerable.t()
  @callback listen(
              stream_name :: Delugex.StreamName.t(),
              opts :: listen_opts()
            ) :: {:ok, listen_ref()} | {:error, any}
  @callback unlisten(
              ref :: listen_ref(),
              opts :: listen_opts()
            ) :: {:ok, listen_ref()} | {:error, any}

  defguard is_version(version) when is_integer(version) and version >= 0

  defguard is_optional_version(version)
           when is_nil(version) or is_version(version)

  defguard is_expected_version(version)
           when version == :no_stream or is_optional_version(version)

  defguard is_batch_size(size) when is_integer(size) and size >= 1

  @spec write_initial!(
          message_store :: module,
          event :: Delugex.Event.t()
        ) :: no_return()
  def write_initial!(message_store, event) do
    message_store.write!(event, :no_stream)
  end

  @spec stream(
          message_store :: module,
          stream_name :: Delugex.StreamName.t(),
          position :: version(),
          batch_size :: batch_size()
        ) :: Enumerable.t()
  def stream(
        message_store,
        stream_name,
        position \\ 0,
        batch_size \\ 10
      )
      when is_version(position) and is_batch_size(batch_size) do
    stream_pos = Position.new(message_store, stream_name, position, batch_size)
    Delugex.MessageStore.Stream.from_position(stream_pos)
  end

  defmacro __using__(_) do
    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def write_initial!(event) do
        unquote(__MODULE__).write_initial!(__MODULE__, event)
      end

      @impl unquote(__MODULE__)
      def stream(stream_name, position \\ 0, batch_size \\ 10) do
        unquote(__MODULE__).stream(
          __MODULE__,
          stream_name,
          position,
          batch_size
        )
      end

      defoverridable write_initial!: 1, stream: 3
    end
  end
end
