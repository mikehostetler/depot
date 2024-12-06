defmodule Depot.Resource do
  @moduledoc """
  Pure data representation of resources in the virtual filesystem.
  Keeps only essential attributes and metadata, with domain logic handled separately.
  """

  @derive [Inspect]
  @type t :: %__MODULE__{
          address: Depot.Address.t(),
          mime_type: String.t() | nil,
          type: :file | :directory,
          size: non_neg_integer() | nil,
          mtime: DateTime.t() | nil,
          metadata: map()
        }

  defstruct [
    :address,
    :mime_type,
    :type,
    :size,
    :mtime,
    metadata: %{}
  ]

  @doc """
  Creates a new resource with the given address and type.
  """
  def new(address, type, opts \\ []) when type in [:file, :directory] do
    %__MODULE__{
      address: address,
      type: type,
      mime_type: Keyword.get(opts, :mime_type),
      size: Keyword.get(opts, :size),
      mtime: Keyword.get(opts, :mtime),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end
end
