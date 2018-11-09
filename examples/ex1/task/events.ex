defmodule Ex1.Task.Events do
  use EspEx.EventTransformer, events: __MODULE__

  defmodule Created do
    use EspEx.Event

    defevent([:campaign_id, :name])
  end

  defmodule Started do
    use EspEx.Event

    defevent([:campaign_id, :start_time])
  end
end
