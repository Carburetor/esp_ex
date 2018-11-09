defmodule Delugex.StreamNameTest do
  use Delugex.Case
  alias Delugex.StreamName

  import Delugex.Stream.Name,
    only: [
      new: 2,
      decode: 1
    ]

  describe "Name.decode" do
    test "campaign:command+position-123" do
      text = decode("campaign:command+position-123")
      map = new("campaign:command+position", "123")

      assert text == map
    end

    test "campaign:position" do
      text = decode("campaign:position")
      map = new("campaign:position", nil)

      assert text == map
    end

    test "campaign:-" do
      text = decode("campaign:-")
      map = new("campaign:", nil)

      assert text == map
    end

    test "campaign---" do
      text = decode("campaign---")
      map = new("campaign", "--")

      assert text == map
    end

    test "campaign:command-123+asd:23" do
      text = decode("campaign:command-123+asd:23")
      map = new("campaign:command", "123+asd:23")

      assert text == map
    end

    test "campaign-123" do
      text = decode("campaign-123")
      map = new("campaign", "123")

      assert text == map
    end

    test "empty string" do
      text = decode("")
      map = new("", nil)

      assert text == map
    end
  end

  describe "Name.to_string" do
    test "appends type after category" do
      map = new("campaign", "123")
      text = "campaign-123"

      assert to_string(map) == text
    end

    test "doesn't append anything when id is nil" do
      map = new("campaign", nil)
      text = "campaign"

      assert to_string(map) == text
    end
  end

  describe "StreamName.category?" do
    test "is true when Name has no id" do
      map = new("campaign", nil)

      assert StreamName.category?(map) == true
    end

    test "is false when Name has id" do
      map = new("campaign", "123")

      assert StreamName.category?(map) == false
    end
  end
end
