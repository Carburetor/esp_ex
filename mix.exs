defmodule EspEx.MixProject do
  use Mix.Project

  @version "VERSION" |> File.read!() |> String.trim()

  def project do
    [
      app: :esp_ex,
      version: @version,
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
      #aliases: [test: "test --no-start"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {EspEx.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 0.5", only: [:dev], runtime: false},
      {:ecto, "~> 2.2"},
      {:postgrex, "~> 0.13"},
      {:jason, "~> 1.0.0"},
      {:uuid, "~> 1.1"}

    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end

#Ecto.Adapters.SQL.query(EspEx.Repo, "select * from messages", [])
