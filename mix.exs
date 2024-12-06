defmodule Depot.MixProject do
  use Mix.Project

  def project do
    [
      app: :depot,
      version: "0.5.2",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "Depot",
      source_url: "https://github.com/elixir-depot/depot",
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  defp description() do
    "A filesystem abstraction for elixir."
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package() do
    [
      # These are the default files included in the package
      files: ~w(lib  mix.exs README* LICENSE* CHANGELOG*),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/elixir-depot/depot"}
    ]
  end

  defp docs do
    [
      groups_for_modules: [
        Stat: [
          ~r/^Depot\.Stat\./
        ],
        Adapters: [
          ~r/^Depot\.Adapter\./
        ]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Depot.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws, "~> 2.1"},
      {:ex_aws_s3, "~> 2.2"},
      {:hackney, "~> 1.9"},
      {:sweet_xml, "~> 0.6"},
      {:minio_server, "~> 0.4.0", only: [:dev, :test]},
      {:jason, "~> 1.0"},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false}
    ]
  end
end
