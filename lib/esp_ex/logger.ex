defmodule EspEx.Logger do
  @moduledoc false

  require Logger
  @tags ["esp_ex"]

  def log(level, chardata_or_fun, metadata \\ []) do
    Logger.log(level, chardata_or_fun, tag(metadata))
  end

  def debug(chardata_or_fun, metadata \\ []) do
    Logger.debug(chardata_or_fun, tag(metadata))
  end

  def info(chardata_or_fun, metadata \\ []) do
    Logger.info(chardata_or_fun, tag(metadata))
  end

  def warn(chardata_or_fun, metadata \\ []) do
    Logger.warn(chardata_or_fun, tag(metadata))
  end

  def error(chardata_or_fun, metadata \\ []) do
    Logger.error(chardata_or_fun, tag(metadata))
  end

  defp tag(metadata) do
    tags = Keyword.get(metadata, :tags, [])
    tags = tags ++ @tags
    Keyword.put(metadata, :tags, tags)
  end
end
