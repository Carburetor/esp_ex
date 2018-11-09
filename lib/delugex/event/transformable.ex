defprotocol Delugex.Event.Transformable do
  @moduledoc """
  Defines how to convert a term into an Event
  """

  @fallback_to_any true

  @doc """
  Fetch the type that will be used as `:type` for Event.Raw
  """
  def type(term_or_text)

  def to_event(term)
  def to_event(term, event_base)
end

defimpl Delugex.Event.Transformable, for: Delugex.Event do
  @spec type(event :: Delugex.Event.t() | String.t()) :: String.t()
  def type(event_or_text) when is_binary(event_or_text), do: event_or_text
  def type(event_or_text), do: event_or_text.type

  def to_event(event), do: event

  @spec to_event(
          event :: Delugex.Event.t(),
          event_base :: Delugex.Event.t()
        ) :: Delugex.Event.t()
  def to_event(event, %Delugex.Event{} = event_base) do
    event_base
    |> Map.put(:data, event.data)
  end

  @spec to_event(
          event :: Delugex.Event.t(),
          stream_name :: Delugex.StreamName.t()
        ) :: Delugex.Event.t()
  def to_event(event, stream_name) do
    event
    |> Map.put(:stream_name, stream_name)
  end
end

defimpl Delugex.Event.Transformable, for: Any do
  @spec type(term_or_text :: any() | String.t()) :: String.t()
  def type(term_or_text), do: Delugex.Event.type(term_or_text)

  def to_event(term), do: Delugex.Event.Transformer.to_event(term)

  @spec to_event(
          term :: any(),
          event_base :: Delugex.Event.t()
        ) :: Delugex.Event.t()
  def to_event(term, %Delugex.Event{} = event_base) do
    Delugex.Event.Transformer.to_event(term, event_base)
  end

  @spec to_event(
          term :: any(),
          stream_name :: Delugex.StreamName.t()
        ) :: Delugex.Event.t()
  def to_event(term, stream_name) do
    Delugex.Event.Transformer.to_event(term, stream_name)
  end
end
