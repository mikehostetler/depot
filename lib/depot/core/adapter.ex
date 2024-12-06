defmodule Depot.Adapter do
  @moduledoc """
  Core adapter behavior for essential filesystem operations.
  """

  @type adapter_config :: struct()
  @type address :: Depot.Address.t()
  @type write_opts :: keyword
  @type stream_opts :: keyword
  @type directory_delete_opts :: keyword
  @type visibility :: Depot.Visibility.t()

  # Setup
  @callback start(adapter_config()) :: :ok | {:ok, pid()} | {:error, term()}
  @callback configure(adapter_config()) :: {module(), adapter_config()}
  @callback supported_capabilities(adapter_config()) :: MapSet.t(Depot.Capability.t())

  # Resource Operations
  @callback read(adapter_config(), address()) :: {:ok, binary()} | {:error, term()}
  @callback write(adapter_config(), address(), contents :: iodata(), write_opts()) ::
              :ok | {:error, term()}
  @callback delete(adapter_config(), address()) :: :ok | {:error, term()}
  @callback move(
              adapter_config(),
              source_address :: address(),
              destination_address :: address(),
              write_opts()
            ) :: :ok | {:error, term()}
  @callback copy(
              source_config :: adapter_config(),
              source_address :: address(),
              destination_address :: address(),
              write_opts(),
              destination_config :: adapter_config() | nil
            ) :: :ok | {:error, term()}
  @callback exists(adapter_config(), address()) :: {:ok, :exists | :missing} | {:error, term()}

  # Visibility
  @callback set_visibility(adapter_config(), address(), visibility()) ::
              :ok | {:error, term()}
  @callback get_visibility(adapter_config(), address()) ::
              {:ok, visibility()} | {:error, term()}

  defmodule Collection do
    @moduledoc """
    Optional adapter behavior for collection (directory) operations.
    """

    @callback list(adapter_config(), address()) ::
                {:ok, [Depot.Resource.t()]} | {:error, term()}
    @callback create_collection(adapter_config(), address()) :: :ok | {:error, term()}
    @callback delete_collection(adapter_config(), address()) :: :ok | {:error, term()}
  end

  defmodule Stream do
    @moduledoc """
    Optional adapter behavior for executable resources.
    """

    @type stream_opts :: keyword

    @callback read_stream(adapter_config(), address(), stream_opts()) ::
                {:ok, Enumerable.t()} | {:error, term()}
    @callback write_stream(adapter_config(), address(), stream_opts()) ::
                {:ok, Collectable.t()} | {:error, term()}
  end

  defmodule Executable do
    @moduledoc """
    Optional adapter behavior for executable resources.
    """

    @callback execute(adapter_config(), address(), term()) ::
                {:ok, term()} | {:error, term()}
  end

  defmodule Subscription do
    @moduledoc """
    Optional adapter behavior for resource change notifications.
    """

    @callback subscribe(adapter_config(), address()) ::
                {:ok, reference()} | {:error, term()}
    @callback unsubscribe(reference()) :: :ok
  end

  defmodule Composite do
    @moduledoc """
    Optional adapter behavior for composite filesystems.
    """
    @callback mount(
                source_config :: adapter_config(),
                destination_config :: adapter_config(),
                address :: address()
              ) :: :ok | {:error, term()}
    @callback unmount(adapter_config(), address()) :: :ok | {:error, term()}
  end
end
