defmodule CubQ.MixProject do
  use Mix.Project

  def project do
    [
      app: :cubq,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      source_url: "https://github.com/lucaong/cubq",
      docs: [
        main: "CubQ"
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test, "coveralls.travis": :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:cubdb, "~> 0.17 or ~> 1.0"},
      {:dialyxir, "~> 1.0.0", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:excoveralls, "~> 0.12", only: :test}
    ]
  end

  defp package() do
    [
      description: "An embedded queue and stack abstraction for Elixir on top of CubDB",
      files: ["lib", "LICENSE", "mix.exs"],
      maintainers: ["Luca Ongaro"],
      licenses: ["Apache-2.0"],
      links: %{}
    ]
  end
end
