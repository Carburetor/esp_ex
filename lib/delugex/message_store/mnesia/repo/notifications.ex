defmodule Delugex.MessageStore.Mnesia.Repo.Notifications do
  defmodule ListenRef do
    @moduledoc false

    @enforce_keys [:stream_name, :listen_ref, :timer_ref]
    defstruct [:stream_name, :listen_ref, :timer_ref]
  end

  alias :mnesia, as: Mnesia

  @interval 10_000

  def listen(stream_name, opts \\ []) do
    {interval} = default_opts(opts)

    %{
      stream_name: stream_name,
      listen: mnesia_listen(),
      timer: timer_listen(interval)
    }
    |> unlisten_if_errored
    |> to_result
  end

  def unlisten(
        %ListenRef{listen_ref: _listen_ref, timer_ref: timer_ref},
        _opts \\ []
      ) do
    {
      mnesia_unlisten(),
      timer_unlisten(timer_ref)
    }
  end

  defp mnesia_listen do
    result = Mnesia.subscribe({:table, Message, :simple})
    Delugex.Logger.debug(fn -> "Mnesia.subscribe #{inspect(self())}" end)
    result
  end

  defp mnesia_unlisten do
    Mnesia.unsubscribe({:table, Message, :simple})
  end

  defp timer_listen(interval) do
    :timer.send_interval(interval, self(), {:reminder})
  end

  defp timer_unlisten(ref) do
    :timer.cancel(ref)
  end

  defp unlisten_if_errored(%{listen: {:ok, _}, timer: {:ok, _}} = listeners) do
    listeners
  end

  defp unlisten_if_errored(%{listen: listener, timer: timer}) do
    listener =
      case listener do
        {:ok, _ref} -> mnesia_unlisten()
        _ -> listener
      end

    timer =
      case timer do
        {:ok, ref} -> timer_unlisten(ref)
        _ -> timer
      end

    %{listen: listener, timer: timer}
  end

  defp to_result(%{
         stream_name: stream_name,
         listen: {:ok, listener},
         timer: {:ok, timer}
       }) do
    {:ok,
     %ListenRef{
       stream_name: stream_name,
       listen_ref: listener,
       timer_ref: timer
     }}
  end

  defp to_result(%{listen: listener, timer: timer}) do
    {:error, {listener, timer}}
  end

  defp default_opts(opts) do
    {
      Keyword.get(opts, :interval, @interval)
    }
  end
end
