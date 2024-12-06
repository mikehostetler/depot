defmodule Depot.Adapter.InMemory do
  @moduledoc """
  Depot Adapter using an `Agent` for in-memory storage.

  ## Direct usage

      iex> {:ok, filesystem} = Depot.Adapter.InMemory.start(name: InMemoryFileSystem)
      iex> :ok = Depot.write(filesystem, "/test.txt", "Hello World")
      iex> {:ok, "Hello World"} = Depot.read(filesystem, "/test.txt")

  ## Usage with a module

      defmodule InMemoryFileSystem do
        use Depot.Filesystem,
          adapter: Depot.Adapter.InMemory
      end

      {:ok, _} = InMemoryFileSystem.start_link([])

      InMemoryFileSystem.write("/test.txt", "Hello World")
      {:ok, "Hello World"} = InMemoryFileSystem.read("/test.txt")
  """

  use Depot.Adapter,
    scheme: "memory",
    capabilities: [:transformable, :collection, :streamable]

  @behaviour Depot.Adapter.Stream
  @behaviour Depot.Adapter.Collection
  defmodule AgentStream do
    @enforce_keys [:config, :path]
    defstruct config: nil, path: nil, chunk_size: 1024

    defimpl Enumerable do
      def reduce(%{config: config, path: path, chunk_size: chunk_size}, acc, fun) do
        case Depot.Adapter.InMemory.read(config, path) do
          {:ok, contents} ->
            Stream.unfold({contents, 0}, fn
              {data, offset} when byte_size(data) > offset ->
                chunk = binary_part(data, offset, min(chunk_size, byte_size(data) - offset))
                {chunk, {data, offset + byte_size(chunk)}}

              _ ->
                nil
            end)
            |> Enumerable.reduce(acc, fun)

          _ ->
            {:halted, acc}
        end
      end

      def count(_), do: {:error, __MODULE__}
      def slice(_), do: {:error, __MODULE__}
      def member?(_, _), do: {:error, __MODULE__}
    end

    defimpl Collectable do
      def into(%{config: config, path: path} = stream) do
        original =
          case Depot.Adapter.InMemory.read(config, path) do
            {:ok, contents} -> contents
            _ -> ""
          end

        fun = fn
          list, {:cont, x} ->
            [x | list]

          list, :done ->
            contents = original <> IO.iodata_to_binary(:lists.reverse(list))
            Depot.Adapter.InMemory.write(config, path, contents, [])
            stream

          _, :halt ->
            :ok
        end

        {[], fun}
      end
    end
  end

  defstruct [:name, :pid, :converter, :visibility]

  @impl true
  def start(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)
    visibility_config = Keyword.get(opts, :visibility, [])

    converter =
      Keyword.get(visibility_config, :converter, Depot.Visibility.PortableUnixVisibilityConverter)

    visibility = visibility_config |> Keyword.drop([:converter]) |> converter.config()

    state = %__MODULE__{
      name: name,
      converter: converter,
      visibility: visibility
    }

    case Agent.start_link(fn -> {%{}, %{}} end, name: via_name(name)) do
      {:ok, pid} -> {:ok, %{state | pid: pid}}
      error -> error
    end
  end

  @impl true
  def read(%__MODULE__{} = state, address) do
    Agent.get(via_name(state.name), fn {files, _} ->
      case Map.fetch(files, local_path(address)) do
        {:ok, {content, _meta}} -> {:ok, content}
        :error -> {:error, :enoent}
      end
    end)
  end

  @impl true
  def write(%__MODULE__{} = state, address, contents, opts \\ [])
      when is_binary(contents) do
    file_visibility = Keyword.get(opts, :visibility, :private)
    dir_visibility = Keyword.get(opts, :directory_visibility, :private)
    file_mode = state.converter.for_file(state.visibility, file_visibility)
    dir_mode = state.converter.for_directory(state.visibility, dir_visibility)

    Agent.update(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)
      file = {contents, %{visibility: file_visibility, mode: file_mode}}
      files = Map.put(files, path, file)
      dirs = ensure_parent_dir(state, dirs, path, opts)
      {files, dirs}
    end)
  end

  @impl true
  def delete(%__MODULE__{} = state, address) do
    Agent.update(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)
      files = Map.delete(files, path)
      {files, dirs}
    end)
  end

  @impl true
  def move(%__MODULE__{} = state, source, destination, opts) do
    Agent.get_and_update(via_name(state.name), fn {files, dirs} ->
      src_path = local_path(source)
      dst_path = local_path(destination)

      case Map.pop(files, src_path) do
        {nil, _} ->
          {{:error, :enoent}, {files, dirs}}

        {file, files} ->
          dirs = ensure_parent_dir(state, dirs, dst_path, opts)
          files = Map.put(files, dst_path, file)
          {:ok, {files, dirs}}
      end
    end)
  end

  @impl true
  def copy(%__MODULE__{} = state, source, destination, opts) do
    Agent.get_and_update(via_name(state.name), fn {files, dirs} ->
      src_path = local_path(source)
      dst_path = local_path(destination)

      case Map.fetch(files, src_path) do
        {:ok, file} ->
          dirs = ensure_parent_dir(state, dirs, dst_path, opts)
          files = Map.put(files, dst_path, file)
          {:ok, {files, dirs}}

        :error ->
          {{:error, :enoent}, {files, dirs}}
      end
    end)
  end

  @impl true
  def exists?(%__MODULE__{} = state, address) do
    Agent.get(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)

      cond do
        Map.has_key?(files, path) -> {:ok, :exists}
        Map.has_key?(dirs, path) -> {:ok, :exists}
        true -> {:ok, :missing}
      end
    end)
  end

  @impl true
  def set_visibility(%__MODULE__{} = state, address, visibility) do
    Agent.get_and_update(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)

      case Map.fetch(files, path) do
        {:ok, {content, meta}} ->
          mode = state.converter.for_file(state.visibility, visibility)
          files = Map.put(files, path, {content, %{meta | visibility: visibility, mode: mode}})
          {:ok, {files, dirs}}

        :error ->
          case Map.fetch(dirs, path) do
            {:ok, meta} ->
              mode = state.converter.for_directory(state.visibility, visibility)
              dirs = Map.put(dirs, path, %{meta | visibility: visibility, mode: mode})
              {:ok, {files, dirs}}

            :error ->
              {{:error, :enoent}, {files, dirs}}
          end
      end
    end)
  end

  @impl true
  def get_visibility(%__MODULE__{} = state, address) do
    Agent.get(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)

      case Map.fetch(files, path) do
        {:ok, {_, %{visibility: visibility}}} ->
          {:ok, visibility}

        :error ->
          case Map.fetch(dirs, path) do
            {:ok, %{visibility: visibility}} -> {:ok, visibility}
            :error -> {:error, :enoent}
          end
      end
    end)
  end

  @impl Depot.Adapter.Collection
  def list(%__MODULE__{} = state, address) do
    Agent.get(via_name(state.name), fn {files, dirs} ->
      prefix = local_path(address)
      # Normalize prefix to just "/" if listing root
      prefix = if prefix in ["/", "/."], do: "/", else: prefix

      resources =
        (Map.keys(files) ++ Map.keys(dirs))
        |> Enum.filter(&String.starts_with?(&1, prefix))
        # Filter out the root directory itself
        |> Enum.reject(&(&1 == "/" || &1 == "/."))
        |> Enum.map(fn path ->
          # Clean up relative path handling
          entry_path =
            if prefix == "/" do
              # For root listings, just use the path directly
              path
            else
              # For subdirectories, handle relative paths
              "/" <> Path.relative_to(path, prefix)
            end

          # Create clean address with absolute path
          entry_address = %Depot.Address{
            scheme: "memory",
            path: entry_path
          }

          cond do
            Map.has_key?(files, path) ->
              {content, meta} = Map.get(files, path)

              Depot.Resource.new(
                entry_address,
                :file,
                size: byte_size(content),
                mtime: DateTime.utc_now(),
                metadata: meta
              )

            Map.has_key?(dirs, path) ->
              meta = Map.get(dirs, path)

              Depot.Resource.new(
                entry_address,
                :directory,
                size: 0,
                mtime: DateTime.utc_now(),
                metadata: meta
              )
          end
        end)
        |> Enum.reject(&is_nil/1)

      {:ok, resources}
    end)
  end

  @impl Depot.Adapter.Collection
  def create_collection(%__MODULE__{} = state, address, opts \\ []) do
    Agent.update(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)

      dirs =
        Map.put(dirs, path, %{
          visibility: Keyword.get(opts, :directory_visibility, :public),
          mode: 0o755
        })

      dirs = ensure_parent_dir(state, dirs, path, opts)

      {files, dirs}
    end)
  end

  @impl Depot.Adapter.Collection
  def delete_collection(%__MODULE__{} = state, address, opts \\ []) do
    recursive = Keyword.get(opts, :recursive, false)

    Agent.get_and_update(via_name(state.name), fn {files, dirs} ->
      path = local_path(address)

      if recursive do
        files = Map.drop(files, Enum.filter(Map.keys(files), &String.starts_with?(&1, path)))
        dirs = Map.drop(dirs, Enum.filter(Map.keys(dirs), &String.starts_with?(&1, path)))
        {:ok, {files, dirs}}
      else
        case Map.pop(dirs, path) do
          {nil, _} ->
            {{:error, :enoent}, {files, dirs}}

          {_, dirs} ->
            if Enum.any?(Map.keys(files) ++ Map.keys(dirs), &String.starts_with?(&1, path <> "/")) do
              {{:error, :eexist}, {files, dirs}}
            else
              {:ok, {files, dirs}}
            end
        end
      end
    end)
  end

  @impl Depot.Adapter.Stream
  def read_stream(%__MODULE__{} = state, address, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, 1024)

    {:ok,
     %AgentStream{
       config: state,
       path: address,
       chunk_size: chunk_size
     }}
  end

  @impl Depot.Adapter.Stream
  def write_stream(%__MODULE__{} = state, address, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, 1024)

    {:ok,
     %AgentStream{
       config: state,
       path: address,
       chunk_size: chunk_size
     }}
  end

  defp local_path(address) do
    address = if is_binary(address), do: Depot.Address.new(address), else: address
    {:ok, normalized_address} = Depot.Address.normalize(address)
    normalized_address.path
  end

  defp ensure_parent_dir(state, dirs, path, opts) do
    parent_path = Path.dirname(path)

    visibility = Keyword.get(opts, :directory_visibility, :private)
    mode = state.converter.for_directory(state.visibility, visibility)

    ensure_directory(state, dirs, parent_path, visibility, mode)
  end

  defp ensure_directory(state, dirs, path, visibility, mode) do
    if Map.has_key?(dirs, path) do
      dirs
    else
      Map.put(dirs, path, %{visibility: visibility, mode: mode})
    end
  end
end
