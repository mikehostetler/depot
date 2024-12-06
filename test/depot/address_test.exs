defmodule Depot.AddressTest do
  use ExUnit.Case, async: true

  describe "new/1" do
    test "creates address from URI string" do
      addr = Depot.Address.new("s3://bucket/path/to/file")
      assert addr.scheme == "s3"
      assert addr.host == "bucket"
      assert addr.path == "/path/to/file"
    end

    test "creates address from legacy path" do
      addr = Depot.Address.new("/path/to/file")
      assert addr.scheme == "memory"
      assert addr.path == "/path/to/file"
      assert addr.host == nil
    end

    test "handles complex URIs" do
      addr = Depot.Address.new("git://user:pass@host:8080/repo/path?branch=main#commit")
      assert addr.scheme == "git"
      assert addr.userinfo == "user:pass"
      assert addr.host == "host"
      assert addr.port == 8080
      assert addr.path == "/repo/path"
      assert addr.query == %{"branch" => "main"}
      assert addr.fragment == "commit"
    end
  end

  describe "normalize/1" do
    test "normalizes paths and removes duplicate slashes" do
      assert {:ok, addr} = Depot.Address.normalize("/path/to/dir")
      assert addr.path == "/path/to/dir"

      assert {:ok, addr} = Depot.Address.normalize("/path//to///dir")
      assert addr.path == "/path/to/dir"

      assert {:ok, addr} = Depot.Address.normalize(%Depot.Address{path: "path/to/dir"})
      assert addr.path == "/path/to/dir"
    end

    test "returns error for traversal attempts" do
      assert {:error, {:path, :traversal}} = Depot.Address.normalize("../path/to/dir")
      assert {:error, {:path, :traversal}} = Depot.Address.normalize("path/../../to/dir")
      assert {:error, {:path, :traversal}} = Depot.Address.normalize("/path/../../../dir")
    end

    test "preserves URI components during normalization" do
      {:ok, addr} =
        "s3://bucket/path//to/../final/dir"
        |> Depot.Address.new()
        |> Depot.Address.normalize()

      assert addr.scheme == "s3"
      assert addr.host == "bucket"
      assert addr.path == "/path/final/dir"
    end
  end

  describe "join/2" do
    test "joins path segments" do
      addr = Depot.Address.new("/base")
      result = Depot.Address.join(addr, "next")
      assert result.path == "/base/next"

      result = Depot.Address.join("/base", "next")
      assert result.path == "/base/next"
    end

    test "preserves URI components when joining" do
      addr = Depot.Address.new("s3://bucket/base")
      result = Depot.Address.join(addr, "next")
      assert result.scheme == "s3"
      assert result.host == "bucket"
      assert result.path == "/base/next"
    end
  end

  describe "join_prefix/2" do
    test "joins prefix with address path" do
      addr = Depot.Address.new("/path/to/dir")
      result = Depot.Address.join_prefix("/prefix", addr)
      assert result.path == "/prefix/path/to/dir"

      result = Depot.Address.join_prefix("/prefix", "/path/to/dir")
      assert result.path == "/prefix/path/to/dir"
    end

    test "handles special cases" do
      addr = Depot.Address.new("/")
      assert Depot.Address.join_prefix("/", addr).path == "/"
      assert Depot.Address.join_prefix("/prefix", addr).path == "/prefix"
    end

    test "preserves URI components when joining prefix" do
      addr = Depot.Address.new("s3://bucket/path")
      result = Depot.Address.join_prefix("/prefix", addr)
      assert result.scheme == "s3"
      assert result.host == "bucket"
      assert result.path == "/prefix/path"
    end
  end

  describe "to_string/1" do
    test "converts simple file paths" do
      addr = Depot.Address.new("/path/to/file")
      assert Depot.Address.to_string(addr) == "/path/to/file"
    end

    test "converts complex URIs" do
      uri = "s3://user:pass@bucket:9000/path/to/file?version=1#fragment"
      addr = Depot.Address.new(uri)
      assert Depot.Address.to_string(addr) == uri
    end
  end
end
