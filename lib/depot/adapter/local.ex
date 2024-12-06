defmodule Depot.Adapter.Local do
  use Depot.Adapter,
    scheme: "file",
    capabilities: [:transformable, :collection, :streamable, :executable]

  use ExDbug, enabled: false

  import Bitwise
  alias Depot.Visibility.UnixVisibilityConverter
  alias Depot.Visibility.PortableUnixVisibilityConverter, as: DefaultVisibilityConverter
  @behaviour Depot.Adapter.Stream
  @behaviour Depot.Adapter.Collection
  @behaviour Depot.Adapter.Executable

  @type t :: %__MODULE__{name: atom(), converter: module(), root_path: binary()}
  defstruct [:name, :converter, :visibility, :root_path]

  @impl true
  def start(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)
    root_path = Keyword.get(opts, :root_path, System.tmp_dir!())
    visibility_config = Keyword.get(opts, :visibility, [])
    converter = Keyword.get(visibility_config, :converter, DefaultVisibilityConverter)
    visibility = visibility_config |> Keyword.drop([:converter]) |> converter.config()

    with :ok <- File.mkdir_p(root_path) do
      {:ok,
       %__MODULE__{name: name, converter: converter, visibility: visibility, root_path: root_path}}
    end
  end

  @impl true
  def read(%__MODULE__{} = state, address) do
    path = local_path(state, address)
    File.read(path)
  end

  @impl Depot.Adapter
  def write(%__MODULE__{} = state, address, contents, opts \\ [])
      when is_binary(contents) do
    path = local_path(state, address)

    mode =
      with {:ok, visibility} <- Keyword.fetch(opts, :visibility) do
        mode = state.converter.for_file(state.visibility, visibility)
        {:ok, mode}
      end

    with :ok <- ensure_parent_dir(state, address, opts),
         :ok <- File.write(path, contents),
         :ok <- maybe_chmod(path, mode) do
      :ok
    end
  end

  @impl true
  def delete(%__MODULE__{} = state, address) do
    path = local_path(state, address)

    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      error -> error
    end
  end

  @impl true
  def move(%__MODULE__{} = state, source, destination, _opts) do
    src_path = local_path(state, source)
    dst_path = local_path(state, destination)

    with :ok <- ensure_parent_dir(state, destination) do
      File.rename(src_path, dst_path)
    end
  end

  @impl true
  def copy(
        %__MODULE__{} = state,
        source,
        destination,
        opts
      ) do
    source = local_path(state, source)
    destination = local_path(state, destination)

    with :ok <- ensure_parent_dir(state, destination) do
      File.cp(source, destination)
    end
  end

  @impl true
  def exists?(%__MODULE__{} = state, address) do
    path = local_path(state, address)
    File.exists?(path) |> then(&{:ok, if(&1, do: :exists, else: :missing)})
  end

  @impl true
  def set_visibility(%__MODULE__{} = state, address, visibility) do
    path = local_path(state, address)

    mode =
      if File.dir?(path) do
        state.converter.for_directory(state.visibility, visibility)
      else
        state.converter.for_file(state.visibility, visibility)
      end

    File.chmod(path, mode)
  end

  @impl true
  def get_visibility(%__MODULE__{} = state, address) do
    path = local_path(state, address)

    with {:ok, %{mode: mode, type: type}} <- File.stat(path) do
      {:ok, visibility_for_mode(state, type, mode)}
    end
  end

  @impl Depot.Adapter.Collection
  def list(%__MODULE__{} = state, address) do
    path = local_path(state, address)

    with {:ok, entries} <- File.ls(path) do
      resources =
        entries
        |> Enum.map(fn entry ->
          full_path = Path.join(path, entry)

          # Create a proper Depot.Address for the entry
          entry_address =
            case address do
              %Depot.Address{} = addr ->
                # Join while preserving the scheme and other URI parts
                Depot.Address.join(addr, entry)

              binary when is_binary(binary) ->
                # Create a new address with proper absolute path
                Depot.Address.new("/" <> entry)
            end

          case File.stat(full_path, time: :posix) do
            {:ok, stat} -> create_resource(state, entry_address, stat)
            {:error, _} -> nil
          end
        end)
        |> Enum.reject(&is_nil/1)

      {:ok, resources}
    end
  end

  @impl Depot.Adapter.Collection
  def create_collection(%__MODULE__{} = state, address, opts \\ []) do
    path = local_path(state, address)
    ensure_directory(state, path, opts)
  end

  @impl Depot.Adapter.Collection
  def delete_collection(%__MODULE__{} = state, address, opts \\ []) do
    path = local_path(state, address)

    if Keyword.get(opts, :recursive, false) do
      with {:ok, _} <- File.rm_rf(path), do: :ok
    else
      File.rmdir(path)
    end
  end

  @impl Depot.Adapter.Stream
  def read_stream(%__MODULE__{} = state, address, opts) when is_list(opts) do
    path = local_path(state, address)
    modes = Keyword.get(opts, :modes, [:read, :binary])
    chunk_size = Keyword.get(opts, :chunk_size, :line)

    case File.exists?(path) do
      true ->
        try do
          {:ok, File.stream!(path, modes, chunk_size)}
        rescue
          e -> {:error, e}
        end

      false ->
        {:error, %File.Error{action: :open_read, path: path}}
    end
  end

  @impl Depot.Adapter.Stream
  def write_stream(%__MODULE__{} = state, address, opts \\ []) do
    path = local_path(state, address)

    with :ok <- ensure_parent_dir(state, address) do
      modes = Keyword.get(opts, :modes, [:write, :binary])
      chunk_size = Keyword.get(opts, :chunk_size, :line)

      try do
        {:ok, File.stream!(path, modes, chunk_size)}
      rescue
        e -> {:error, e}
      end
    end
  end

  # Private helpers

  defp local_path(%__MODULE__{} = state, address) do
    address = if is_binary(address), do: Depot.Address.new(address), else: address
    {:ok, normalized_address} = Depot.Address.normalize(address)
    Path.join(state.root_path, normalized_address.path)
  end

  defp create_resource(state, address, %File.Stat{} = stat) do
    type =
      case stat.type do
        :regular -> :file
        :directory -> :directory
        _ -> nil
      end

    visibility =
      case stat.mode &&& 0o777 do
        0o600 -> :private
        _ -> :public
      end

    if type do
      Depot.Resource.new(
        address,
        type,
        size: stat.size,
        mtime: DateTime.from_unix!(stat.mtime),
        metadata: %{
          executable: (stat.mode &&& 0o111) > 0,
          watchable: true,
          visibility: visibility_for_mode(state, type, stat.mode)
        }
      )
    end
  end

  defp visibility_for_mode(state, type, mode) do
    mode = mode &&& 0o777

    case type do
      :directory -> state.converter.from_directory(state.visibility, mode)
      _ -> state.converter.from_file(state.visibility, mode)
    end
  end

  defp ensure_parent_dir(%__MODULE__{} = state, address, opts \\ []) do
    path = local_path(state, address) |> Path.dirname()
    ensure_directory(state, path, opts)
  end

  defp ensure_directory(config, path, opts) do
    mode =
      with {:ok, visibility} <- Keyword.fetch(opts, :directory_visibility) do
        mode = config.converter.for_directory(config.visibility, visibility)
        {:ok, mode}
      end

    path
    |> IO.chardata_to_string()
    |> Path.join("/")
    |> do_mkdir_p(mode)
  end

  defp do_mkdir_p(path, mode) do
    with :missing <- existing_directory(path),
         parent = Path.dirname(path),
         :ok <- infinite_loop_protect(path),
         :ok <- do_mkdir_p(parent, mode),
         :ok <- :file.make_dir(path) do
      maybe_chmod(path, mode)
    end
  end

  defp existing_directory(path) do
    if File.dir?(path), do: :ok, else: :missing
  end

  defp infinite_loop_protect(path) do
    if Path.dirname(path) != path, do: :ok, else: {:error, :einval}
  end

  defp maybe_chmod(path, {:ok, mode}), do: File.chmod(path, mode)
  defp maybe_chmod(_path, :error), do: :ok
end
