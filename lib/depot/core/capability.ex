defmodule Depot.Capability do
  @moduledoc """
  Pure functions for checking resource capabilities based on metadata.
  """

  @capability_keys [:executable, :watchable, :versioned, :transformable]

  @doc """
  Checks if a resource has a specific capability.
  """
  def has_capability?(%Depot.Resource{} = resource, capability)
      when capability in @capability_keys do
    Map.get(resource.metadata, capability, false)
  end

  @doc """
  Adds a capability to a resource.
  """
  def add_capability(%Depot.Resource{} = resource, capability)
      when capability in @capability_keys do
    update_in(resource.metadata, &Map.put(&1, capability, true))
  end
end
