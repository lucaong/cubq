defmodule CubQ.MixProject do
  use Mix.Project

  def project do
    [
      app: :cubq,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:dialyxir, "~> 1.0.0", only: [:dev], runtime: false}
    ]
  end
end
