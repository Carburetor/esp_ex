defmodule Delugex.MessageStore.Mnesia.Repo do
  @moduledoc false

  defmodule InvalidStreamNameError do
    defexception [:message]
  end

  alias :mnesia, as: Mnesia
  alias Delugex.StreamName
  alias Delugex.MessageStore.Mnesia.ExpectedVersionError
  require Ex2ms

  @message_attrs [
    :global_position,
    # category, id, global, local
    :stream_all,
    # category, id, local
    :stream_local,
    :id,
    :stream_name,
    :stream_category,
    :stream_id,
    :type,
    :position,
    :data,
    :metadata,
    :time
  ]
  # + 1 erlang is 1-based, + 1 first term is table name
  @message_stream_local_idx Enum.find_index(@message_attrs, fn attr ->
                              attr == :stream_local
                            end) + 2

  def create do
    with {:atomic, _} <-
           Mnesia.create_table(
             Message,
             attributes: @message_attrs,
             index: [:id, :stream_all, :stream_local],
             type: :ordered_set
           ),
         {:atomic, _} <-
           Mnesia.create_table(
             Message.Position,
             attributes: [:stream, :position]
           ),
         {:atomic, _} <-
           Mnesia.create_table(
             Message.Global,
             attributes: [:key, :value]
           ),
         {:atomic, _} <-
           Mnesia.transaction(fn ->
             :ok = Mnesia.write({Message.Global, :global_position, 0})
           end) do
      :ok
    else
      error -> error
    end
  end

  def delete do
    Mnesia.delete_table(Message)
    Mnesia.delete_table(Message.Position)
    Mnesia.delete_table(Message.Global)
    :ok
  end

  def write_message([id, stream_name, type, data, metadata, expected_version]) do
    Mnesia.transaction(fn ->
      Mnesia.write_lock_table(Message)
      Mnesia.write_lock_table(Message.Position)
      Mnesia.write_lock_table(Message.Global)
      {:atomic, version} = stream_version(stream_name)
      {:atomic, global} = new_global_position()
      {:atomic, local} = new_local_position(stream_name)

      case same_version?(version, expected_version) do
        false ->
          Mnesia.abort(%ExpectedVersionError{
            message: "#{expected_version} != #{version}"
          })

        true ->
          stream_category = StreamName.category(stream_name)
          stream_id = StreamName.id(stream_name)

          :ok =
            Mnesia.write({
              Message,
              global,
              {stream_category, stream_id, global, local},
              {stream_category, stream_id, local},
              id,
              encode_stream_name(stream_name),
              stream_category,
              stream_id,
              to_string(type),
              local,
              encode_json(data),
              encode_json(metadata),
              NaiveDateTime.utc_now()
            })

          local
      end
    end)
  end

  def write_messages(messages) do
    Mnesia.transaction(fn ->
      Enum.reduce(messages, nil, fn message, _ ->
        {:atomic, version} = write_message(message)
        version
      end)
    end)
  end

  def stream_version(stream_name) do
    Mnesia.transaction(fn ->
      target = to_stream(stream_name)

      case wget({Message.Position, target}) do
        {_, _, current_pos} -> current_pos
        nil -> nil
      end
    end)
  end

  def get_category_messages(_stream_name, _global_pos, 0),
    do: {[], :"$end_of_table"}

  def get_category_messages(stream_name, global_pos, batch_size) do
    if !StreamName.category?(stream_name),
      do: raise(InvalidStreamNameError, message: "Stream name not a category")

    category_name = StreamName.category(stream_name)

    spec =
      Ex2ms.fun do
        {_table, _global, {stream_category, _p_stream_id, global, _p_local},
         _stream_local, _id, _stream_name, _stream_category, _stream_id, _type,
         _local, _data, _metadata, _time} = record
        when stream_category == ^category_name and global >= ^global_pos ->
          record
      end

    Mnesia.transaction(fn ->
      case Mnesia.select(Message, spec, batch_size, :read) do
        {records, cont} -> {Enum.map(records, &decode/1), cont}
        :"$end_of_table" -> {[], :"$end_of_table"}
        error -> error
      end
    end)
  end

  def get_stream_messages(_stream_name, _local_pos, 0),
    do: {[], :"$end_of_table"}

  def get_stream_messages(stream_name, local_pos, batch_size) do
    if StreamName.category?(stream_name),
      do: raise(InvalidStreamNameError, message: "Stream name is a category")

    id = StreamName.id(stream_name)
    category = StreamName.category(stream_name)

    spec =
      Ex2ms.fun do
        {_table, _global, {stream_category, stream_id, _p_global, local},
         _stream_local, _id, _stream_name, _stream_category, _stream_id, _type,
         _local, _data, _metadata, _time} = record
        when stream_category == ^category and stream_id == ^id and
               local >= ^local_pos ->
          record
      end

    Mnesia.transaction(fn ->
      case Mnesia.select(Message, spec, batch_size, :read) do
        {records, cont} -> {Enum.map(records, &decode/1), cont}
        :"$end_of_table" -> {[], :"$end_of_table"}
        error -> error
      end
    end)
  end

  def get_last_message(stream_name) do
    if StreamName.category?(stream_name),
      do: raise(InvalidStreamNameError, message: "Stream name is a category")

    Mnesia.transaction(fn ->
      id = StreamName.id(stream_name)
      category = StreamName.category(stream_name)
      target = to_stream(stream_name)

      records =
        case get({Message.Position, target}) do
          nil ->
            []

          {_, _, pos} ->
            stream_local = {category, id, pos}
            Mnesia.index_read(Message, stream_local, @message_stream_local_idx)
        end

      records
      |> List.first()
      |> decode()
    end)
  end

  defp same_version?(_version, nil), do: true
  defp same_version?(nil, -1), do: true

  defp same_version?(version, expected) when is_integer(version) do
    version == expected
  end

  defp new_local_position(stream_name) do
    Mnesia.transaction(fn ->
      target = to_stream(stream_name)

      current_pos =
        case wget({Message.Position, target}) do
          nil -> -1
          {_, _, pos} -> pos
        end

      new_pos = current_pos + 1
      :ok = Mnesia.write({Message.Position, target, new_pos})
      new_pos
    end)
  end

  defp new_global_position do
    Mnesia.transaction(fn ->
      {_, _, current_pos} = wget({Message.Global, :global_position})

      new_pos = current_pos + 1
      :ok = Mnesia.write({Message.Global, :global_position, new_pos})
      new_pos
    end)
  end

  defp wget(record, default \\ nil), do: get(record, default, :write)

  defp get({tab, key}, default \\ nil, lock \\ :read) do
    case Mnesia.read(tab, key, lock) do
      [] -> default
      [record] -> record
    end
  end

  defp decode(nil), do: nil

  defp decode({
         _table,
         global_position,
         _stream_all,
         _stream_local,
         id,
         stream_name,
         _stream_category,
         _stream_id,
         type,
         position,
         data,
         metadata,
         time
       }) do
    [id, stream_name, type, position, global_position, data, metadata, time]
  end

  defp to_stream(stream_name) do
    id = StreamName.id(stream_name)
    category = StreamName.category(stream_name)
    {category, id}
  end

  defp encode_json(term) do
    encoder =
      __MODULE__
      |> Delugex.Config.get(:json, [])
      |> Keyword.get(:encoder, Jason)

    encoder.encode!(term)
  end

  defp encode_stream_name(term) do
    StreamName.to_string(term)
  end
end
