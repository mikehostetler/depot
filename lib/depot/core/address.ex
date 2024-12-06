defmodule Depot.Address do
  @moduledoc """
  Abstract representation of resource locations supporting various schemes and backends.
  This module is a superset of the Elixir URI module, providing additional functionality
  for handling complex resource addresses and maintaining backwards compatibility with path operations.

  Format: [scheme]://[userinfo]@[host]:[port][path]?[query]#[fragment]

  Examples:
  - file:///path/to/resource
  - s3://bucket/key
  - git://repo/path
  - /path/to/resource (legacy path format)
  """

  @derive [Inspect]
  defstruct [:scheme, :userinfo, :host, :port, :path, :query, :fragment]

  @type t :: %__MODULE__{
          scheme: nil | binary,
          userinfo: nil | binary,
          host: nil | binary,
          port: nil | :inet.port_number(),
          path: nil | binary,
          query: nil | binary | map,
          fragment: nil | binary
        }

  @default_scheme "memory"

  @doc """
  Creates a new address from a URI string, parts, or legacy path.
  """
  @spec new(binary | URI.t() | t()) :: t()
  def new(uri) when is_binary(uri) do
    case URI.parse(uri) do
      %URI{scheme: nil} = parsed ->
        %__MODULE__{scheme: @default_scheme, path: ensure_absolute(uri)}

      parsed ->
        new(parsed)
    end
  end

  def new(%URI{} = uri) do
    %__MODULE__{
      scheme: uri.scheme || @default_scheme,
      userinfo: uri.userinfo,
      host: uri.host,
      port: uri.port,
      path: ensure_absolute(uri.path || "/"),
      query: parse_query(uri.query),
      fragment: uri.fragment
    }
  end

  def new(%__MODULE__{} = addr), do: addr

  @doc """
  Normalizes an address path, resolving '..' and '.' segments.
  """
  @spec normalize(t() | binary) :: {:ok, t()} | {:error, {:path, :traversal}}
  def normalize(path) when is_binary(path) do
    normalize(%__MODULE__{scheme: @default_scheme, path: path})
  end

  def normalize(%__MODULE__{} = addr) do
    case expand_path(addr.path) do
      {:ok, expanded_path} ->
        normalized = %{addr | path: String.replace(expanded_path, ~r|/+|, "/")}
        {:ok, normalized}

      {:error, :traversal} ->
        {:error, {:path, :traversal}}
    end
  end

  @doc """
  Joins path segments in an address.
  """
  @spec join(t() | binary, binary) :: t()
  def join(addr, segment2) when is_binary(addr) do
    join(%__MODULE__{scheme: @default_scheme, path: addr}, segment2)
  end

  def join(%__MODULE__{} = addr, segment2) do
    joined_path =
      [addr.path, segment2]
      |> Enum.map(&String.trim(&1, "/"))
      |> Enum.reject(&(&1 == ""))
      |> case do
        [] -> "/"
        parts -> "/" <> Enum.join(parts, "/")
      end

    %{addr | path: joined_path}
  end

  @doc """
  Joins a prefix with an address path.
  """
  @spec join_prefix(binary, t() | binary) :: t()
  def join_prefix(prefix, addr) when is_binary(addr) do
    join_prefix(prefix, %__MODULE__{scheme: @default_scheme, path: addr})
  end

  def join_prefix(prefix, %__MODULE__{} = addr) do
    prefix_path = ensure_absolute(prefix)
    path = ensure_absolute(addr.path)

    joined_path =
      case {prefix_path, path} do
        {"/", _} -> path
        {pre, "/"} -> pre
        {pre, pth} -> Path.join(pre, String.trim_leading(pth, "/"))
      end
      |> String.replace(~r|/+|, "/")

    %{addr | path: joined_path}
  end

  @doc """
  Converts an address to string representation.
  """
  @spec to_string(t()) :: binary
  def to_string(%__MODULE__{} = addr) do
    if addr.scheme == @default_scheme && !addr.host && !addr.userinfo && !addr.query &&
         !addr.fragment do
      addr.path
    else
      URI.to_string(%URI{
        scheme: addr.scheme,
        userinfo: addr.userinfo,
        host: addr.host,
        port: addr.port,
        path: addr.path,
        query: encode_query(addr.query),
        fragment: addr.fragment
      })
    end
  end

  # Private helper functions

  defp expand_path(path) do
    case expand_segments(Path.split(ensure_absolute(path)), []) do
      {:ok, []} -> {:ok, "/"}
      {:ok, result} -> {:ok, "/" <> Enum.join(result, "/")}
      {:error, :traversal} -> {:error, :traversal}
    end
  end

  defp expand_segments([], acc), do: {:ok, Enum.reverse(acc)}
  defp expand_segments([".." | _], []), do: {:error, :traversal}
  defp expand_segments([".." | rest], [_ | acc]), do: expand_segments(rest, acc)
  defp expand_segments([".", "/" | rest], acc), do: expand_segments(rest, acc)
  defp expand_segments(["." | rest], acc), do: expand_segments(rest, acc)
  defp expand_segments(["/" | rest], acc), do: expand_segments(rest, acc)
  defp expand_segments([seg | rest], acc), do: expand_segments(rest, [seg | acc])

  defp ensure_absolute(nil), do: "/"
  defp ensure_absolute(""), do: "/"
  defp ensure_absolute("/" <> _ = path), do: path
  defp ensure_absolute(path), do: "/" <> path

  defp parse_query(nil), do: nil
  defp parse_query(query) when is_binary(query), do: URI.decode_query(query)

  defp encode_query(nil), do: nil
  defp encode_query(query) when is_map(query), do: URI.encode_query(query)
end
