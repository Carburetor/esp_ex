defmodule Delugex.Consumer.State do
  @moduledoc false

  @type t :: %Delugex.Consumer.State{
          listener: {:ok, Delugex.MessageStore.listen_ref()} | {:error, any},
          position: non_neg_integer(),
          global_position: non_neg_integer(),
          events: list(Delugex.Event.Raw.t()),
          meta: any
        }
  defstruct listener: nil,
            position: 0,
            global_position: 0,
            events: [],
            meta: nil
end
