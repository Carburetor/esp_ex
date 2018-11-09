defmodule Delugex.MixProject do
  use Mix.Project

  @version "VERSION" |> File.read!() |> String.trim()

  def project do
    [
      app: :delugex,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      test_paths: ["test/automated"],
      dialyzer: [
        plt_add_apps: [:mnesia],
        flags: [
          :unmatched_returns,
          :error_handling,
          :race_conditions
        ],
        paths: ["_build/#{Mix.env()}/lib/delugex/consolidated"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, ">= 0.5.0", only: [:dev], runtime: false},
      {:ecto, ">= 3.0.0"},
      {:ecto_sql, ">= 3.0.0"},
      {:postgrex, ">= 0.14.0"},
      {:jason, ">= 1.1.0"},
      {:ex2ms, ">= 1.5.0"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/automated/support"]
  defp elixirc_paths(_), do: ["lib"]
end
