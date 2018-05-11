defmodule EspEx.EventBus.Postgres do
  @moduledoc """
  This is the real implementation of EventBus. It will execute the needed
  queries on Postgres through Postgrex by calling the functions provided in
  [ESP](https://github.com/Carburetor/ESP/tree/master/app/config/functions/stream). You should be able to infer what to write, it's just passing the
  required arguments to the SQL functions and converting any returned value.
  Whenever a stream name is expected, please use the %StreamName struct and
  make sure to convert it to string.
  """

  use EspEx.EventBus

  @doc """
  Write has an expected_version argument. This argument could be one of:
  - None: no version expected
  - NoStream: no message ever written to this stream, the Postgres
    stream_version position will return null (max(position) is null if no rows
    are present)
  - A number (0+): Representing the expected version
  """
  def write(args_here) do
  end

  @doc """
  listen will start listening for a specific stream name using Postgres LISTEN.
  Check Postgrex to see how to use Postgres LISTEN
  """
  def listen(args_here) do
  end

  @doc """
  unlisten stops Postgres LISTEN
  """
  def unlisten(args_here) do
  end
end
