defmodule EspEx.Event do
  @moduledoc """
  Converts a generic event into a RawEvent
  """

  alias EspEx.RawEvent
  alias EspEx.StreamName

  def to_raw_event(event) when is_map(event) do
    to_raw_event(event, %RawEvent{})
  end

  @spec to_raw_event(
          event :: struct(),
          raw_event_base :: EspEx.RawEvent.t()
        ) :: EspEx.RawEvent.t()
  def to_raw_event(event, %RawEvent{} = raw_event_base) when is_map(event) do
    id = raw_event_base.id || random_uuid()
    time = raw_event_base.time || naive_datetime_now()
    stream_name = raw_event_base.stream_name
    type = raw_event_base.type || type(event)

    raw_event =
      raw_event_base
      |> Map.put(:id, id)
      |> Map.put(:stream_name, stream_name)
      |> Map.put(:type, type)
      |> Map.put(:data, Map.from_struct(event))
      |> Map.put(:time, time)

    EspEx.Logger.debug(fn ->
      "Event #{inspect(event)} converted to #{inspect(raw_event)}"
    end)

    raw_event
  end

  @spec to_raw_event(
          event :: struct(),
          stream_name :: EspEx.StreamName.t()
        ) :: EspEx.RawEvent.t()
  def to_raw_event(event, %StreamName{} = stream_name) when is_map(event) do
    raw_event_base = %RawEvent{stream_name: stream_name}

    to_raw_event(event, raw_event_base)
  end

  @spec type(text :: String.t()) :: String.t()
  def type(text) when is_bitstring(text), do: text

  @spec type(event :: struct) :: String.t()
  def type(%{__struct__: module}) do
    module
    |> to_string()
    |> Module.split()
    |> List.last()
  end

  defp random_uuid do
    UUID.uuid4()
  end

  defp naive_datetime_now do
    DateTime.utc_now()
    |> DateTime.to_naive()
  end
end
