defmodule Depot.Adapter do
  @moduledoc """
  Core adapter behavior for essential filesystem operations.
  """

  @type adapter :: struct()
  @type address :: Depot.Address.t() | binary()
  @type write_opts :: keyword()
  @type stream_opts :: keyword()
  @type directory_delete_opts :: keyword()
  @type visibility :: Depot.Visibility.t()

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour Depot.Adapter

      # Extract scheme and capabilities from options or raise if missing
      @scheme Keyword.fetch!(unquote(opts), :scheme)
      @capabilities Keyword.get(unquote(opts), :capabilities, [])

      # Common interface functions
      def scheme, do: @scheme
      def capabilities, do: @capabilities

      @doc """
      Returns a :via tuple for registration with Depot.Registry
      """
      def via_name(name) do
        {:via, Registry, {Depot.Registry, {__MODULE__, name}}}
      end

      @doc """
      Normalizes the path in an Address struct
      """

      def normalize_path(%Depot.Address{} = address) do
        %{address | path: Depot.Address.normalize_path(address)}
      end

      @doc """
      Creates a new resource with proper metadata
      """

      def create_resource(base_address, relative_path, content, meta) do
        type = if is_map(content), do: :directory, else: :file
        size = if is_binary(content), do: byte_size(content), else: 0

        address = Depot.Address.merge(base_address, %Depot.Address{path: relative_path})

        Depot.Resource.new(
          ensure_scheme(address),
          type,
          size: size,
          mtime: meta.mtime,
          metadata: %{transformable: true}
        )
      end

      @doc """
      Ensures address has correct scheme
      """
      def ensure_scheme(%Depot.Address{} = address) do
        %{address | scheme: @scheme}
      end

      @doc """
      Resolves a path relative to the adapter's root path
      """
      def resolve_path(config, %Depot.Address{} = address) do
        normalized_address = normalize_path(address)
        root_address = %Depot.Address{path: config.root_path}

        case Depot.Address.merge(root_address, normalized_address) do
          %Depot.Address{path: path} = resolved_address ->
            if String.starts_with?(path, config.root_path) do
              {:ok, resolved_address}
            else
              {:error, :invalid_path}
            end
        end
      end

      # Default implementations that can be overridden
      def supported_capabilities(_config) do
        MapSet.new([:transformable])
      end

      # Required callback stubs that must be implemented
      defoverridable supported_capabilities: 1

      def read(adapter, address) do
        raise "#{__MODULE__}.read/2 not implemented"
      end

      def write(adapter, address, contents, opts) do
        raise "#{__MODULE__}.write/4 not implemented"
      end

      def delete(adapter, address) do
        raise "#{__MODULE__}.delete/2 not implemented"
      end

      def move(adapter, source, destination, opts) do
        raise "#{__MODULE__}.move/4 not implemented"
      end

      def copy(adapter, source, destination, opts) do
        raise "#{__MODULE__}.copy/4 not implemented"
      end

      def exists?(adapter, address) do
        raise "#{__MODULE__}.exists?/2 not implemented"
      end

      def set_visibility(adapter, address, visibility) do
        raise "#{__MODULE__}.set_visibility/3 not implemented"
      end

      def get_visibility(adapter, address) do
        raise "#{__MODULE__}.get_visibility/2 not implemented"
      end

      defoverridable read: 2,
                     write: 4,
                     delete: 2,
                     move: 4,
                     copy: 4,
                     exists?: 2,
                     set_visibility: 3,
                     get_visibility: 2
    end
  end

  # Core behavior callbacks
  @callback start(keyword()) :: :ok | {:ok, pid()} | {:error, term()}
  @callback capabilities() :: Depot.Capability.Adapter.t()

  # Optional behavior callbacks
  @callback read(adapter(), address()) :: {:ok, binary()} | {:error, term()}
  @callback write(adapter(), address(), contents :: iodata(), write_opts()) ::
              :ok | {:error, term()}
  @callback delete(adapter(), address()) :: :ok | {:error, term()}
  @callback move(
              adapter(),
              source_address :: address(),
              destination_address :: address(),
              write_opts()
            ) :: :ok | {:error, term()}
  @callback copy(
              source_adapter :: adapter(),
              source_address :: address(),
              destination_address :: address(),
              write_opts()
            ) :: :ok | {:error, term()}
  @callback exists?(adapter(), address()) :: {:ok, :exists | :missing} | {:error, term()}
  @callback set_visibility(adapter(), address(), visibility()) ::
              :ok | {:error, term()}
  @callback get_visibility(adapter(), address()) ::
              {:ok, visibility()} | {:error, term()}

  defmodule Collection do
    @moduledoc """
    Optional adapter behavior for collection (directory) operations.
    """
    @type adapter :: Depot.Adapter.adapter()
    @type address :: Depot.Address.t()

    @callback list(adapter(), address()) ::
                {:ok, [Depot.Resource.t()]} | {:error, term()}
    @callback create_collection(adapter(), address(), keyword()) :: :ok | {:error, term()}
    @callback delete_collection(adapter(), address(), keyword()) :: :ok | {:error, term()}
  end

  defmodule Stream do
    @moduledoc """
    Optional adapter behavior for executable resources.
    """
    @type adapter :: Depot.Adapter.adapter()
    @type address :: Depot.Address.t()
    @type stream_opts :: keyword

    @callback read_stream(adapter(), address(), stream_opts()) ::
                {:ok, Enumerable.t()} | {:error, term()}
    @callback write_stream(adapter(), address(), stream_opts()) ::
                {:ok, Collectable.t()} | {:error, term()}
  end

  defmodule Executable do
    @moduledoc """
    Optional adapter behavior for executable resources.
    """
    @type adapter :: Depot.Adapter.adapter()
    @type address :: Depot.Address.t()

    @callback execute(adapter(), address(), term()) ::
                {:ok, term()} | {:error, term()}
  end

  defmodule Mountable do
    @moduledoc """
    Optional adapter behavior for composite filesystems.
    """
    @type adapter :: Depot.Adapter.adapter()
    @type address :: Depot.Address.t()

    @callback mount(
                source_config :: adapter(),
                destination_config :: adapter(),
                address :: address()
              ) :: :ok | {:error, term()}
    @callback unmount(adapter(), address()) :: :ok | {:error, term()}
  end
end
