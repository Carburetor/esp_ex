defmodule Delugex.MessageStore.Postgres.Repo do
  defmodule ListenRef do
    @moduledoc false

    @enforce_keys [:listen_ref, :timer_ref]
    defstruct [:listen_ref, :timer_ref]
  end

  use Ecto.Repo,
    otp_app: :delugex,
    adapter: Ecto.Adapters.Postgres

  alias Postgrex.Notifications, as: PG

  @timeout 5000
  @interval 10_000

  @doc """
  Dynamically loads the repository url from the
  DELUGEX_DATABASE_URL environment variable.
  """
  def init(_, opts) do
    {:ok, Keyword.put(opts, :url, System.get_env("DELUGEX_DATABASE_URL"))}
  end

  def listen(stream_name, opts \\ []) do
    {timeout, interval} = default_opts(opts)

    %{
      listen: pg_listen(stream_name, timeout),
      timer: timer_listen(interval)
    }
    |> unlisten_if_errored
    |> to_result
  end

  def unlisten(
        %ListenRef{listen_ref: listen_ref, timer_ref: timer_ref},
        opts \\ []
      ) do
    {timeout, _} = default_opts(opts)

    {
      pg_unlisten(listen_ref, timeout),
      timer_unlisten(timer_ref)
    }
  end

  defp pg_listen(stream_name, timeout) do
    result = PG.listen(PG, stream_name, timeout: timeout)
    Delugex.Logger.debug(fn -> "Postgrex.listen #{inspect(self())}" end)
    result
  end

  defp pg_unlisten(ref, timeout) do
    PG.unlisten(PG, ref, timeout: timeout)
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
        {:ok, ref} -> pg_unlisten(ref, timeout: @timeout)
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
         listen: {:ok, listener},
         timer: {:ok, timer}
       }) do
    {:ok, %ListenRef{listen_ref: listener, timer_ref: timer}}
  end

  defp to_result(%{listen: listener, timer: timer}) do
    {:error, {listener, timer}}
  end

  defp default_opts(opts) do
    {
      Keyword.get(opts, :timeout, @timeout),
      Keyword.get(opts, :interval, @interval)
    }
  end
end
