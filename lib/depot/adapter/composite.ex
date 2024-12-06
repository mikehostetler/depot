defmodule Depot.Adapter.Composite do
  @moduledoc """
  A composite adapter that can mount and unmount other adapters on specific paths.
  It delegates access to @adapter.ex callbacks to the mounted adapters.
  """

  use Depot.Adapter, scheme: "composite"
  @behaviour Depot.Adapter.Mountable

  # Type definitions
  @type t :: %__MODULE__{
          mounts: %{Depot.Address.t() => {Depot.Adapter.adapter(), Depot.Address.t()}}
        }
  defstruct [:mounts]

  @impl true
  def start(opts) when is_list(opts) do
    {:ok, %__MODULE__{mounts: %{}}}
  end

  @impl true
  def supported_capabilities(_config) do
    MapSet.new([:transformable, :mountable])
  end

  @impl Depot.Adapter.Composite
  def mount(config, source_config, destination_address) do
    new_mounts = Map.put(config.mounts, destination_address, {source_config, destination_address})
    {:ok, %{config | mounts: new_mounts}}
  end

  @impl Depot.Adapter.Composite
  def unmount(config, address) do
    new_mounts = Map.delete(config.mounts, address)
    {:ok, %{config | mounts: new_mounts}}
  end

  # Helper function to find the appropriate mounted adapter for a given address
  defp find_adapter(config, address) do
    Enum.find_value(config.mounts, fn {mount_path, {adapter, _}} ->
      if String.starts_with?(address.path, mount_path.path) do
        relative_path = Path.relative_to(address.path, mount_path.path)
        {adapter, %{address | path: relative_path}}
      end
    end)
  end

  # Implement Depot.Adapter callbacks

  @impl true
  def read(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.read(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  @impl true
  def write(config, address, contents, opts) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.write(adapter, relative_address, contents, opts)
      nil -> {:error, :not_found}
    end
  end

  @impl true
  def delete(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.delete(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  @impl true
  def move(config, source, destination, opts) do
    case {find_adapter(config, source), find_adapter(config, destination)} do
      {{source_adapter, source_address}, {dest_adapter, dest_address}}
      when source_adapter == dest_adapter ->
        source_adapter.move(source_adapter, source_address, dest_address, opts)

      {{source_adapter, source_address}, {dest_adapter, dest_address}} ->
        with {:ok, content} <- source_adapter.read(source_adapter, source_address),
             :ok <- dest_adapter.write(dest_adapter, dest_address, content, opts),
             :ok <- source_adapter.delete(source_adapter, source_address) do
          :ok
        else
          error -> error
        end

      _ ->
        {:error, :not_found}
    end
  end

  @impl true
  def copy(config, source, destination, opts, destination_adapter \\ nil) do
    case {find_adapter(config, source), find_adapter(config, destination)} do
      {{source_adapter, source_address}, {dest_adapter, dest_address}} ->
        source_adapter.copy(source_adapter, source_address, dest_address, opts, dest_adapter)

      _ ->
        {:error, :not_found}
    end
  end

  @impl true
  def exists?(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.exists?(adapter, relative_address)
      nil -> {:ok, :missing}
    end
  end

  @impl true
  def set_visibility(config, address, visibility) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.set_visibility(adapter, relative_address, visibility)
      nil -> {:error, :not_found}
    end
  end

  @impl true
  def get_visibility(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.get_visibility(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  # Implement Depot.Collection behavior
  @impl Depot.Collection
  def list(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.list(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  @impl Depot.Collection
  def create_collection(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.create_collection(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  @impl Depot.Collection
  def delete_collection(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.delete_collection(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end

  # Implement Depot.Stream behavior
  @impl Depot.Stream
  def read_stream(config, address, opts) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.read_stream(adapter, relative_address, opts)
      nil -> {:error, :not_found}
    end
  end

  @impl Depot.Stream
  def write_stream(config, address, opts) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.write_stream(adapter, relative_address, opts)
      nil -> {:error, :not_found}
    end
  end

  # Implement Depot.Executable behavior
  @impl Depot.Executable
  def set_executable(config, address, executable) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.set_executable(adapter, relative_address, executable)
      nil -> {:error, :not_found}
    end
  end

  @impl Depot.Executable
  def get_executable(config, address) do
    case find_adapter(config, address) do
      {adapter, relative_address} -> adapter.get_executable(adapter, relative_address)
      nil -> {:error, :not_found}
    end
  end
end
