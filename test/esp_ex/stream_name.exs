defmodule EspEx.StreamNameTest do
  use ExUnit.Case, async: true
  doctest EspEx.StreamName
  alias EspEx.StreamName

  import EspEx.StreamName,
    only: [
      new: 3,
      new: 2,
      new: 1,
      from_string: 1
    ]

  #
  describe "StreamName.from_string" do
    test "campaign:command+position-123" do
      text = from_string("campaign:command+position-123")
      map = new("campaign", "123", ["command", "position"])

      assert text == map
    end

    test "campaign:position+command-123" do
      text = from_string("campaign:position+command-123")
      map = new("campaign", "123", ["command", "position"])

      assert text == map
    end

    test "campaign:position-123" do
      text = from_string("campaign:position-123")
      map = new("campaign", "123", ["position"])

      assert text == map
    end

    test "campaign:position" do
      text = from_string("campaign:position")
      map = new("campaign", nil, ["position"])

      assert text == map
    end

    test "campaign:position+command" do
      text = from_string("campaign:position+command")
      map = new("campaign", nil, ["command", "position"])

      assert text == map
    end

    test "campaign:-" do
      text = from_string("campaign:-")
      map = new("campaign", nil, [])

      assert text == map
    end

    test "campaign:" do
      text = from_string("campaign:")
      map = new("campaign", nil, [])

      assert text == map
    end

    test "campaign-" do
      text = from_string("campaign-")
      map = new("campaign", nil, [])

      assert text == map
    end

    test "campaign+" do
      text = from_string("campaign+")
      map = new("campaign+", nil, [])

      assert text == map
    end

    test "campaign" do
      text = from_string("campaign")
      map = new("campaign", nil, [])

      assert text == map
    end

    test "+" do
      text = from_string("+")
      map = new("+", nil, [])

      assert text == map
    end

  test "campaign---" do
    text = from_string("campaign---")
    map = new("campaign", "--", [])

    assert text == map
  end

    test "campaign:command-123+asd:23" do
      text = from_string("campaign:command-123+asd:23")
      map = new("campaign", "123+asd:23", ["command"])

      assert text == map
    end

    test "campaign:command+" do
      text = from_string("campaign:command+")
      map = new("campaign", nil, ["command"])

      assert text == map
    end

    test "campaign-123" do
      text = from_string("campaign-123")
      map = new("campaign", "123", [])

      assert text == map
    end

    test "campaign-123-asd-456" do
      text = from_string("campaign-123-asd-456")
      map = new("campaign", "123-asd-456", [])

      assert text == map
    end

    test "raises with empty string" do
      assert_raise ArgumentError, fn ->
        from_string("")
      end
    end

    test "raises with a blank string" do
      assert_raise ArgumentError, fn ->
        from_string("           ")
      end
    end

    test "raises with just : since it's blank" do
      assert_raise ArgumentError, fn ->
        from_string("   :  ")
      end
    end

    test "raises with just - since it's blank" do
      assert_raise ArgumentError, fn ->
        from_string("   -  ")
      end
    end

    test "raises with just :- since it's blank" do
      assert_raise ArgumentError, fn ->
        from_string("   :-  ")
      end
    end

    test "raises with just :- even without spaces around" do
      assert_raise ArgumentError, fn ->
        from_string(":-")
      end
    end
  end

  describe "StreamName.new" do

    test "creates a struct with new and an arity of 1" do
      struct = %StreamName{category: "campaign"}
      map = new("campaign")
      assert struct == map
    end

    test "creates a struct with new and an arity of 2" do
      struct = %StreamName{category: "campaign", identifier: "123"}
      map = new("campaign", "123")
      assert struct == map
    end

    test "creates a struct with new and an arity of 3" do
      struct = %StreamName{category: "campaign",
                          identifier: "123",
                          types: :ordsets.from_list(["command", "position"])
                          }
      map = new("campaign", "123", ["command", "position"])
      assert struct == map
    end
    test "raises when category is blank" do
      assert_raise ArgumentError, fn ->
        new("       ")
      end
    end

    # NOTE: Look into this one again..
    test "raises when category is not a string" do
      assert_raise FunctionClauseError, fn ->
        struct = new(123)
        assert struct.category == 123
      end
    end

    test "raises when category is nil" do
      assert_raise FunctionClauseError, fn ->
        new(nil)
      end
    end

    test "raises when identifier is not nil or string" do
      assert_raise FunctionClauseError, fn ->
        new("campaign", %{something: 123})
      end
    end

    test "raises when types is not a list" do
      assert_raise FunctionClauseError, fn ->
        new("campaign", "asd-123", %{wrong: 123})
      end
    end
  end

  describe "StreamName.to_string" do
    test "has types always in the same order as in alphabetized order" do
      map = new("campaign", "123", ["position", "command"])
      text = "campaign:command+position-123"

      assert to_string(map) == text
    end
  end

  describe "StreamName.has_all_types" do
    test "is true when types is a sublist of StreamName types" do
      map = new("campaign", nil, ["position", "command", "snapshot"])

      assert StreamName.has_all_types(map, ["position", "snapshot"]) == true
    end

    test "is true when types is empty" do
      map = new("campaign", nil, ["position", "command", "snapshot"])

      assert StreamName.has_all_types(map, []) == true
    end

    test "is true when types is empty and StreamName has no types" do
      map = new("campaign", nil, [])

      assert StreamName.has_all_types(map, []) == true
    end

    test "is true when types has one type and StreamName has no types" do
      map = new("campaign", nil, ["command"])

      assert StreamName.has_all_types(map, []) == true
    end

    test "is true when types matches StreamName types" do
      map = new("campaign", nil, ["command", "position"])

      assert StreamName.has_all_types(map, ["position", "command"]) == true
    end

    test "is false when types don't match StreamName types" do
      map = new("campaign", nil, ["position"])

      assert StreamName.has_all_types(map, ["command"]) == false
    end
  end

  describe "StreamName.is_category" do
    test "is true when StreamName has no identifier but has types" do
      map = new("campaign", nil, ["position", "command", "snapshot"])

      assert StreamName.is_category(map) == true
    end

    test "is true when StreamName has no identifier and no types" do
      map = new("campaign", nil, [])

      assert StreamName.is_category(map) == true
    end

    test "is false when StreamName has identifier" do
      map = new("campaign", "123", ["command"])

      assert StreamName.is_category(map) == false
    end
  end

  describe "StreamName.position_identifier" do
    test "raises when position is less than 0" do
      map = new("campaign", "123", ["command"])

      assert_raise ArgumentError, fn ->
        StreamName.position_identifier(map, -1)
      end
    end

    test "raises when position is not an integer or nil" do
      map = new("campaign", "123", ["command"])

      assert_raise ArgumentError, fn ->
        StreamName.position_identifier(map, 2.3)
      end
    end

    test "returns campaign:command-123/1 when position is 1" do
      map = new("campaign", "123", ["command"])
      text = "campaign:command-123/1"

      assert StreamName.position_identifier(map, 1) == text
    end

    test "returns campaign:command-123 when position is nil" do
      map = new("campaign", "123", ["command"])
      text = "campaign:command-123"

      assert StreamName.position_identifier(map, nil) == text
    end
  end
end