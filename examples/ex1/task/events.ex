defmodule Ex1.Task.Events do
  use Delugex.EventTransformer, events: __MODULE__

  defmodule Created do
    use Delugex.Event

    defevent([:campaign_id, :name])
  end

  defmodule Started do
    use Delugex.Event

    defevent([:campaign_id, :start_time])
  end
end
