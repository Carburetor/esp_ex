defmodule EspEx.Consumer.State do
  @moduledoc false

  @type t :: %EspEx.Consumer.State{
          listener: {:ok, EspEx.MessageStore.listen_ref()} | {:error, any},
          position: non_neg_integer(),
          global_position: non_neg_integer(),
          events: list(EspEx.RawEvent.t()),
          meta: any
        }
  defstruct listener: nil,
            position: 0,
            global_position: 0,
            events: [],
            meta: nil
end
