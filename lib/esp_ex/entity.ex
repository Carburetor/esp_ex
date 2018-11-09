defmodule EspEx.Entity do
  @type t :: struct

  @callback new() :: t
end
