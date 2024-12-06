defmodule Depot.Address do
  @moduledoc """
  Abstract representation of resource locations supporting various schemes and backends.
  This module is a superset of the Elixir URI module, providing additional functionality
  for handling complex resource addresses.

  Format: [scheme]://[userinfo]@[host]:[port][path]?[query]#[fragment]

  Examples: file:///path/to/resource, s3://bucket/key, git://repo/path
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

  @doc """
  Creates a new address from a URI string or parts.
  """
  @spec new(binary | URI.t()) :: t()
  def new(uri) when is_binary(uri) do
    uri
    |> URI.parse()
    |> new()
  end

  def new(%URI{} = uri) do
    %__MODULE__{
      scheme: uri.scheme || "file",
      userinfo: uri.userinfo,
      host: uri.host,
      port: uri.port,
      path: uri.path || "/",
      query: parse_query(uri.query),
      fragment: uri.fragment
    }
  end

  @doc """
  Converts an address back to string representation.
  """
  @spec to_string(t()) :: binary
  def to_string(%__MODULE__{} = addr) do
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

  @doc """
  Parses the query string into a map.
  """
  @spec parse_query(nil | binary) :: nil | map
  defp parse_query(nil), do: nil
  defp parse_query(query) when is_binary(query), do: URI.decode_query(query)

  @doc """
  Encodes a query map back to a string.
  """
  @spec encode_query(nil | map) :: nil | binary
  defp encode_query(nil), do: nil
  defp encode_query(query) when is_map(query), do: URI.encode_query(query)

  @doc """
  Merges two Address structs, with the second taking precedence.
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = addr1, %__MODULE__{} = addr2) do
    Map.merge(addr1, addr2, fn
      :query, q1, q2 when is_map(q1) and is_map(q2) -> Map.merge(q1, q2)
      _, _, v2 -> v2
    end)
  end

  @doc """
  Checks if the address is absolute (has a scheme).
  """
  @spec absolute?(t()) :: boolean
  def absolute?(%__MODULE__{scheme: scheme}) when is_binary(scheme), do: true
  def absolute?(_), do: false

  @doc """
  Checks if the address is relative (no scheme).
  """
  @spec relative?(t()) :: boolean
  def relative?(addr), do: not absolute?(addr)
end
