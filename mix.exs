defmodule Rsmqx.MixProject do
  use Mix.Project

  @source_url "https://github.com/v0idpwn-industries/rsmqx"
  @version "0.1.0"

  def project do
    [
      app: :rsmqx,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Implementation of RSMQ in Elixir",
      package: package(),
      name: "Rsmqx",
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      maintainers: ["Andre LMS", "Felipe Stival"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(mix.exs README.md CHANGELOG.md lib priv)
    ]
  end

  defp docs do
    [
      main: "Rsmqx",
      source_ref: "v#{@version}",
      source_url: @source_url,
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:redix, "~> 1.1"},
      {:uuid, "~> 1.1", only: [:test, :dev]},
      {:ex_doc, "~> 0.20", only: :docs}
    ]
  end
end
