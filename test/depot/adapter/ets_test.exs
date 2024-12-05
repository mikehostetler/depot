defmodule Depot.Adapter.ETSTest do
  use ExUnit.Case, async: true
  import Depot.AdapterTest
  # doctest Depot.Adapter.ETS

  setup do
    filesystem = Depot.Adapter.ETS.configure(name: :ets_test)
    start_supervised!(filesystem)
    {:ok, filesystem: filesystem}
  end

  adapter_test %{filesystem: filesystem} do
    {:ok, filesystem: filesystem}
  end

  describe "write" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "test.txt", "Hello World", [])

      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "test.txt")
    end

    test "folders are automatically created if missing", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "folder/test.txt", "Hello World", [])

      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "folder/test.txt")
    end

    test "visibility", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "public.txt", "Hello World", visibility: :public)
      :ok = Depot.Adapter.ETS.write(config, "private.txt", "Hello World", visibility: :private)

      assert {:ok, :public} = Depot.Adapter.ETS.visibility(config, "public.txt")
      assert {:ok, :private} = Depot.Adapter.ETS.visibility(config, "private.txt")
    end
  end

  describe "read" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "test.txt", "Hello World", [])

      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "test.txt")
    end

    test "file not found", %{filesystem: filesystem} do
      {_, config} = filesystem

      assert {:error, :enoent} = Depot.Adapter.ETS.read(config, "nonexistent.txt")
    end
  end

  describe "delete" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "test.txt", "Hello World", [])
      assert :ok = Depot.Adapter.ETS.delete(config, "test.txt")
      assert {:error, :enoent} = Depot.Adapter.ETS.read(config, "test.txt")
    end

    test "successful even if no file to delete", %{filesystem: filesystem} do
      {_, config} = filesystem

      assert :ok = Depot.Adapter.ETS.delete(config, "nonexistent.txt")
    end
  end

  describe "move" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "source.txt", "Hello World", [])
      assert :ok = Depot.Adapter.ETS.move(config, "source.txt", "destination.txt", [])
      assert {:error, :enoent} = Depot.Adapter.ETS.read(config, "source.txt")
      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "destination.txt")
    end
  end

  describe "copy" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "source.txt", "Hello World", [])
      assert :ok = Depot.Adapter.ETS.copy(config, "source.txt", "destination.txt", [])
      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "source.txt")
      assert {:ok, "Hello World"} = Depot.Adapter.ETS.read(config, "destination.txt")
    end
  end

  describe "file_exists" do
    test "existing file", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "test.txt", "Hello World", [])
      assert {:ok, :exists} = Depot.Adapter.ETS.file_exists(config, "test.txt")
    end

    test "non-existing file", %{filesystem: filesystem} do
      {_, config} = filesystem

      assert {:ok, :missing} = Depot.Adapter.ETS.file_exists(config, "nonexistent.txt")
    end
  end

  describe "list_contents" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.write(config, "file1.txt", "Content 1", [])
      :ok = Depot.Adapter.ETS.write(config, "file2.txt", "Content 2", [])
      :ok = Depot.Adapter.ETS.create_directory(config, "dir1", [])

      {:ok, contents} = Depot.Adapter.ETS.list_contents(config, ".")

      assert Enum.any?(contents, fn item -> item.name == "file1.txt" end)
      assert Enum.any?(contents, fn item -> item.name == "file2.txt" end)
      assert Enum.any?(contents, fn item -> item.name == "dir1" end)
    end
  end

  describe "create_directory" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      assert :ok = Depot.Adapter.ETS.create_directory(config, "new_dir", [])
      assert {:ok, :exists} = Depot.Adapter.ETS.file_exists(config, "new_dir")
    end
  end

  describe "delete_directory" do
    test "success", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.create_directory(config, "dir_to_delete", [])
      assert :ok = Depot.Adapter.ETS.delete_directory(config, "dir_to_delete", [])
      assert {:ok, :missing} = Depot.Adapter.ETS.file_exists(config, "dir_to_delete")
    end

    test "recursive delete", %{filesystem: filesystem} do
      {_, config} = filesystem

      :ok = Depot.Adapter.ETS.create_directory(config, "parent_dir", [])
      :ok = Depot.Adapter.ETS.write(config, "parent_dir/file.txt", "Content", [])

      assert :ok = Depot.Adapter.ETS.delete_directory(config, "parent_dir", recursive: true)
      assert {:ok, :missing} = Depot.Adapter.ETS.file_exists(config, "parent_dir")
      assert {:ok, :missing} = Depot.Adapter.ETS.file_exists(config, "parent_dir/file.txt")
    end
  end
end
