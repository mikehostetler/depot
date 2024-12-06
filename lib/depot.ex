defmodule Depot do
  @external_resource "README.md"
  @moduledoc @external_resource
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  @type adapter :: module()
  @type filesystem :: {module(), Depot.Adapter.config()}

  @type capability :: Depot.Capability.t()

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      {adapter, opts} = Depot.parse_opts(__MODULE__, opts)

      @adapter adapter
      @opts opts

      def __filesystem__, do: struct(@adapter, @opts)

      def read(path), do: Depot.read(__filesystem__(), path)

      def write(path, contents, opts \\ []),
        do: Depot.write(__filesystem__(), path, contents, opts)

      def delete(path), do: Depot.delete(__filesystem__(), path)

      def move(source, destination, opts \\ []),
        do: Depot.move(__filesystem__(), source, destination, opts)

      def copy(source, destination, opts \\ []),
        do: Depot.copy(__filesystem__(), source, destination, opts)

      def exists?(path, opts \\ []), do: Depot.exists?(__filesystem__(), path, opts)

      def set_visibility(path, visibility, opts \\ []),
        do: Depot.set_visibility(__filesystem__(), path, visibility, opts)

      def get_visibility(path, opts \\ []), do: Depot.get_visibility(__filesystem__(), path, opts)

      # Optional Collection behavior
      def list(path, opts \\ []), do: Depot.list(__filesystem__(), path, opts)

      def create_collection(path, opts \\ []),
        do: Depot.create_collection(__filesystem__(), path)

      def delete_collection(path, opts \\ []),
        do: Depot.delete_collection(__filesystem__(), path, opts)

      # Optional Stream behavior
      def read_stream(path, opts \\ []), do: Depot.read_stream(__filesystem__(), path, opts)
      def write_stream(path, opts \\ []), do: Depot.write_stream(__filesystem__(), path, opts)

      # Optional Executable behavior
      def execute(path, args \\ []), do: Depot.execute(__filesystem__(), path, args)

      # Optional Mountable behavior
      def mount(source_config, destination_address, opts \\ []),
        do: Depot.mount(__filesystem__(), source_config, destination_address, opts)

      def unmount(address, opts \\ []), do: Depot.unmount(__filesystem__(), address, opts)

      def supports?(capability), do: Depot.supports?(__filesystem__(), capability)
    end
  end

  def parse_opts(module, opts) do
    opts
    |> merge_app_env(module)
    |> Keyword.put_new(:name, module)
    |> Keyword.pop!(:adapter)
  end

  def merge_app_env(opts, module) do
    case Keyword.fetch(opts, :otp_app) do
      {:ok, otp_app} ->
        config = Application.get_env(otp_app, module, [])
        Keyword.merge(opts, config)

      :error ->
        opts
    end
  end

  @doc """
  Start the adapter with the given options.

  ## Examples

      {:ok, filesystem} = Depot.start(MyAdapter, name: MyFs, root_path: "/tmp")

  """
  @spec start(module(), keyword()) :: :ok | {:ok, pid()} | {:error, term()}
  def start(adapter, opts) do
    adapter.start(opts)
  end

  @doc """
  Get the capabilities of the adapter.

  ## Examples

      capabilities = Depot.capabilities(MyAdapter)

  """
  @spec capabilities(module()) :: Depot.Capability.Adapter.t()
  def capabilities(adapter) do
    adapter.capabilities()
  end

  @doc """
  Write to a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.write(filesystem, "test.txt", "Hello World")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      LocalFileSystem.write("test.txt", "Hello World")

  """
  @spec write(filesystem, Depot.Address.t() | binary(), iodata(), keyword()) ::
          :ok | {:error, term()}
  def write(filesystem, address, contents, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.write(filesystem, address, contents, opts)
    end)
  end

  @doc """
  Read from a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, content} = Depot.read(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, content} = LocalFileSystem.read("test.txt")

  """
  @spec read(filesystem, Depot.Address.t() | binary()) ::
          {:ok, binary()} | {:error, term()}
  def read(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.read(filesystem, address)
    end)
  end

  @doc """
  Delete a file from a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.delete(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.delete("test.txt")

  """
  @spec delete(filesystem, Depot.Address.t() | binary()) :: :ok | {:error, term()}
  def delete(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.delete(filesystem, address)
    end)
  end

  @doc """
  Move a file within a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.move(filesystem, "source.txt", "destination.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.move("source.txt", "destination.txt")

  """
  @spec move(filesystem, Depot.Address.t() | binary(), Depot.Address.t() | binary(), keyword()) ::
          :ok | {:error, term()}
  def move(filesystem, source, destination, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.move(filesystem, source, destination, opts)
    end)
  end

  @doc """
  Copy a file within a filesystem or between filesystems

  ## Examples

  ### Within the same filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.copy(filesystem, "source.txt", "destination.txt")

  ### Between different filesystems

      source_fs = Depot.Adapter.Local.configure(prefix: "/home/user/source")
      dest_fs = Depot.Adapter.S3.configure(bucket: "my-bucket")
      :ok = Depot.copy(source_fs, "source.txt", dest_fs, "destination.txt")

  """

  @spec copy(
          filesystem,
          Depot.Address.t() | binary(),
          Depot.Address.t() | binary(),
          keyword()
        ) ::
          :ok | {:error, term()}
  def copy(filesystem, source, destination, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.copy(filesystem, source, destination, opts)
    end)
  end

  @doc """
  Check if a file exists in a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, :exists} = Depot.exists?(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, :exists} = LocalFileSystem.exists?("test.txt")

  """
  @spec exists?(filesystem, Depot.Address.t() | binary()) ::
          {:ok, :exists | :missing} | {:error, term()}
  def exists?(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.exists?(filesystem, address)
    end)
  end

  @doc """
  Set the visibility of a file in a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.set_visibility(filesystem, "test.txt", :public)

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.set_visibility("test.txt", :public)

  """
  @spec set_visibility(filesystem, Depot.Address.t() | binary(), Depot.Adapter.visibility()) ::
          :ok | {:error, term()}
  def set_visibility(filesystem, address, visibility) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.set_visibility(filesystem, address, visibility)
    end)
  end

  @doc """
  Get the visibility of a file in a filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, :public} = Depot.get_visibility(filesystem, "test.txt")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, :public} = LocalFileSystem.get_visibility("test.txt")

  """
  @spec get_visibility(filesystem, Depot.Address.t() | binary()) ::
          {:ok, Depot.Adapter.visibility()} | {:error, term()}
  def get_visibility(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:transformable)
    |> then(fn adapter ->
      adapter.get_visibility(filesystem, address)
    end)
  end

  @doc """
  List contents of a directory in the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, resources} = Depot.list(filesystem, "documents")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, resources} = LocalFileSystem.list("documents")

  """
  @spec list(filesystem, Depot.Address.t() | binary()) ::
          {:ok, [Depot.Resource.t()]} | {:error, term()}
  def list(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:collection)
    |> then(fn adapter ->
      adapter.list(filesystem, address)
    end)
  end

  @doc """
  Create a new directory in the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.create_collection(filesystem, "new_folder")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.create_collection("new_folder")

  """
  @spec create_collection(filesystem, Depot.Address.t() | binary()) ::
          :ok | {:error, term()}
  def create_collection(filesystem, address, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:collection)
    |> then(fn adapter ->
      adapter.create_collection(filesystem, address, opts)
    end)
  end

  @doc """
  Delete a directory and its contents from the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.delete_collection(filesystem, "old_folder")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.delete_collection("old_folder")

  """
  @spec delete_collection(filesystem, Depot.Address.t() | binary(), keyword()) ::
          :ok | {:error, term()}
  def delete_collection(filesystem, address, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:collection)
    |> then(fn adapter ->
      adapter.delete_collection(filesystem, address, opts)
    end)
  end

  @doc """
  Creates a read stream for a file in the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, stream} = Depot.read_stream(filesystem, "large_file.txt")
      Enum.each(stream, &IO.puts/1)

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, stream} = LocalFileSystem.read_stream("large_file.txt")
      Enum.each(stream, &IO.puts/1)

  """
  @spec read_stream(filesystem, Depot.Address.t() | binary(), Depot.Adapter.stream_opts()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(filesystem, address, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:streamable)
    |> then(fn adapter ->
      adapter.read_stream(filesystem, address, opts)
    end)
  end

  @doc """
  Creates a write stream for a file in the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, stream} = Depot.write_stream(filesystem, "new_large_file.txt")
      Enum.into(["Hello", "World"], stream)

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, stream} = LocalFileSystem.write_stream("new_large_file.txt")
      Enum.into(["Hello", "World"], stream)

  """
  @spec write_stream(filesystem, Depot.Address.t() | binary(), Depot.Adapter.stream_opts()) ::
          {:ok, Collectable.t()} | {:error, term()}
  def write_stream(filesystem, address, opts \\ []) do
    filesystem
    |> get_adapter()
    |> has_capability?(:streamable)
    |> then(fn adapter ->
      adapter.write_stream(filesystem, address, opts)
    end)
  end

  @doc """
  Execute a command on a file in the filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      {:ok, result} = Depot.execute(filesystem, "script.sh", ["arg1", "arg2"])

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      {:ok, result} = LocalFileSystem.execute("script.sh", ["arg1", "arg2"])

  """
  @spec execute(filesystem, Depot.Address.t() | binary(), term()) ::
          {:ok, term()} | {:error, term()}
  def execute(filesystem, address, args) do
    filesystem
    |> get_adapter()
    |> has_capability?(:executable)
    |> then(fn adapter ->
      adapter.execute(filesystem, address, args)
    end)
  end

  @doc """
  Mount a filesystem to another filesystem

  ## Examples

  ### Direct filesystem

      source_fs = Depot.Adapter.Local.configure(prefix: "/home/user/source")
      dest_fs = Depot.Adapter.S3.configure(bucket: "my-bucket")
      :ok = Depot.mount(source_fs, dest_fs, "mounted")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      defmodule S3FileSystem do
        use Depot,
          adapter: Depot.Adapter.S3,
          bucket: "my-bucket"
      end

      :ok = LocalFileSystem.mount(S3FileSystem, "mounted")

  """
  @spec mount(filesystem, filesystem, Depot.Address.t() | binary()) ::
          :ok | {:error, term()}
  def mount(source_config, destination_config, address) do
    source_config
    |> get_adapter()
    |> has_capability?(:mountable)
    |> then(fn adapter ->
      adapter.mount(source_config, destination_config, address)
    end)
  end

  @doc """
  Unmount a filesystem from another filesystem

  ## Examples

  ### Direct filesystem

      filesystem = Depot.Adapter.Local.configure(prefix: "/home/user/storage")
      :ok = Depot.unmount(filesystem, "mounted")

  ### Module-based filesystem

      defmodule LocalFileSystem do
        use Depot,
          adapter: Depot.Adapter.Local,
          root_path: "/home/user/storage"
      end

      :ok = LocalFileSystem.unmount("mounted")

  """
  @spec unmount(filesystem, Depot.Address.t() | binary()) ::
          :ok | {:error, term()}
  def unmount(filesystem, address) do
    filesystem
    |> get_adapter()
    |> has_capability?(:mountable)
    |> then(fn adapter ->
      adapter.unmount(filesystem, address)
    end)
  end

  defp get_adapter({module, _config}) when is_atom(module), do: module
  defp get_adapter(%struct{} = filesystem) when is_struct(filesystem), do: struct

  defp has_capability?(adapter, capability) do
    adapter_capabilities = adapter.capabilities()

    if capability in adapter_capabilities do
      adapter
    else
      raise "Adapter #{inspect(adapter)} does not support capability #{inspect(capability)}"
    end
  end
end
