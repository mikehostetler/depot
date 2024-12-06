defmodule Depot.Adapter.LocalTest do
  use ExUnit.Case, async: true
  import Depot.AdapterTest
  doctest Depot.Adapter.Local

  @moduletag :tmp_dir

  adapter_test %{tmp_dir: prefix} do
    {:ok, filesystem} = Depot.Adapter.Local.start(name: :my_local_fs, root_path: prefix)
    {:ok, filesystem: filesystem}
  end

  setup %{tmp_dir: prefix} do
    {:ok, prefix: prefix}
  end

  describe "basic adapter methods" do
    setup %{tmp_dir: prefix} do
      {:ok, config} = Depot.Adapter.Local.start(name: :my_local_fs, root_path: prefix)
      {:ok, config: config}
    end

    test "read", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/test.txt", "Hello World")
      assert {:ok, "Hello World"} = Depot.Adapter.Local.read(config, "/test.txt")
      assert {:error, :enoent} = Depot.Adapter.Local.read(config, "/non_existent.txt")
    end

    test "write", %{config: config} do
      assert :ok = Depot.Adapter.Local.write(config, "/new_file.txt", "New content")
      assert {:ok, "New content"} = Depot.Adapter.Local.read(config, "/new_file.txt")
      assert :ok = Depot.Adapter.Local.write(config, "/new_file.txt", "Updated content")
      assert {:ok, "Updated content"} = Depot.Adapter.Local.read(config, "/new_file.txt")
      assert :ok = Depot.Adapter.Local.write(config, "/nested/path/file.txt", "Nested content")
      assert {:ok, "Nested content"} = Depot.Adapter.Local.read(config, "/nested/path/file.txt")
    end

    test "delete", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/to_delete.txt", "Delete me")
      assert :ok = Depot.Adapter.Local.delete(config, "/to_delete.txt")
      assert {:error, :enoent} = Depot.Adapter.Local.read(config, "/to_delete.txt")
      assert :ok = Depot.Adapter.Local.delete(config, "/non_existent.txt")
    end

    test "move", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/source.txt", "Move me")
      assert :ok = Depot.Adapter.Local.move(config, "/source.txt", "/destination.txt", [])
      assert {:error, :enoent} = Depot.Adapter.Local.read(config, "/source.txt")
      assert {:ok, "Move me"} = Depot.Adapter.Local.read(config, "/destination.txt")

      assert {:error, :enoent} =
               Depot.Adapter.Local.move(config, "/non_existent.txt", "/new_place.txt", [])
    end

    test "copy", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/original.txt", "Copy me")
      assert :ok = Depot.Adapter.Local.copy(config, "/original.txt", "/copy.txt", [])
      assert {:ok, "Copy me"} = Depot.Adapter.Local.read(config, "/original.txt")
      assert {:ok, "Copy me"} = Depot.Adapter.Local.read(config, "/copy.txt")

      assert {:error, :enoent} =
               Depot.Adapter.Local.copy(config, "/non_existent.txt", "/new_copy.txt", [])
    end

    test "exists?", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/exists.txt", "I exist")
      assert {:ok, :exists} = Depot.Adapter.Local.exists?(config, "/exists.txt")
      assert {:ok, :missing} = Depot.Adapter.Local.exists?(config, "/does_not_exist.txt")
    end
  end

  describe "stream operations" do
    setup %{tmp_dir: prefix} do
      {:ok, config} = Depot.Adapter.Local.start(name: :my_local_fs, root_path: prefix)
      {:ok, config: config}
    end

    test "read_stream", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/stream_read.txt", "Line 1\nLine 2\nLine 3")

      {:ok, stream} =
        Depot.Adapter.Local.read_stream(config, %Depot.Address{path: "/stream_read.txt"}, [])

      assert Enum.to_list(stream) == ["Line 1\n", "Line 2\n", "Line 3"]

      {:ok, custom_stream} =
        Depot.Adapter.Local.read_stream(config, %Depot.Address{path: "/stream_read.txt"},
          modes: [:read, :utf8],
          chunk_size: 2
        )

      assert Enum.to_list(custom_stream) == [
               "Li",
               "ne",
               " 1",
               "\nL",
               "in",
               "e ",
               "2\n",
               "Li",
               "ne",
               " 3"
             ]

      # Test reading a non-existent file
      assert {:error, %File.Error{action: :open_read, path: _}} =
               Depot.Adapter.Local.read_stream(
                 config,
                 %Depot.Address{path: "/non_existent.txt"},
                 []
               )
    end

    test "write_stream", %{config: config} do
      stream = Stream.map(1..3, &"Line #{&1}\n")

      {:ok, file_stream} =
        Depot.Adapter.Local.write_stream(config, %Depot.Address{path: "/stream_write.txt"}, [])

      Enum.into(stream, file_stream)

      assert {:ok, "Line 1\nLine 2\nLine 3\n"} =
               Depot.Adapter.Local.read(config, "/stream_write.txt")

      custom_stream = Stream.map(1..3, &"Custom #{&1}\n")

      {:ok, custom_file_stream} =
        Depot.Adapter.Local.write_stream(config, %Depot.Address{path: "/custom_stream_write.txt"},
          modes: [:write, :utf8],
          chunk_size: 2
        )

      Enum.into(custom_stream, custom_file_stream)

      assert {:ok, "Custom 1\nCustom 2\nCustom 3\n"} =
               Depot.Adapter.Local.read(config, "/custom_stream_write.txt")

      # Test writing to a non-existent directory
      {:ok, _} =
        Depot.Adapter.Local.write_stream(
          config,
          %Depot.Address{path: "/non_existent_dir/file.txt"},
          []
        )

      # Verify that the directory was created
      assert {:ok, :exists} = Depot.Adapter.Local.exists?(config, "/non_existent_dir")
    end
  end

  describe "collection operations" do
    setup %{tmp_dir: prefix} do
      {:ok, config} = Depot.Adapter.Local.start(name: :my_local_fs, root_path: prefix)
      {:ok, config: config}
    end

    test "list", %{config: config} do
      :ok = Depot.Adapter.Local.write(config, "/list_test/file1.txt", "Content 1")
      :ok = Depot.Adapter.Local.write(config, "/list_test/file2.txt", "Content 2")

      :ok =
        Depot.Adapter.Local.create_collection(config, %Depot.Address{path: "/list_test/subdir"})

      {:ok, entries} = Depot.Adapter.Local.list(config, %Depot.Address{path: "/list_test"})

      assert length(entries) == 3

      assert Enum.any?(entries, fn entry ->
               entry.address.path == "/list_test/file1.txt" and entry.type == :file
             end)

      assert Enum.any?(entries, fn entry ->
               entry.address.path == "/list_test/file2.txt" and entry.type == :file
             end)

      assert Enum.any?(entries, fn entry ->
               entry.address.path == "/list_test/subdir" and entry.type == :directory
             end)

      file_entry =
        Enum.find(entries, fn entry -> entry.address.path == "/list_test/file1.txt" end)

      assert is_integer(file_entry.size)
      assert %DateTime{} = file_entry.mtime
      assert file_entry.metadata.watchable == true
      assert is_boolean(file_entry.metadata.executable)

      assert {:error, :enoent} =
               Depot.Adapter.Local.list(config, %Depot.Address{path: "/non_existent_dir"})
    end

    test "create_collection", %{config: config} do
      assert :ok = Depot.Adapter.Local.create_collection(config, %Depot.Address{path: "/new_dir"})
      assert {:ok, :exists} = Depot.Adapter.Local.exists?(config, "/new_dir")
      assert :ok = Depot.Adapter.Local.create_collection(config, %Depot.Address{path: "/new_dir"})

      assert :ok =
               Depot.Adapter.Local.create_collection(config, %Depot.Address{
                 path: "/parent/child/grandchild"
               })

      assert {:ok, :exists} = Depot.Adapter.Local.exists?(config, "/parent/child/grandchild")
    end

    test "delete_collection", %{config: config} do
      :ok = Depot.Adapter.Local.create_collection(config, %Depot.Address{path: "/delete_dir"})
      :ok = Depot.Adapter.Local.write(config, "/delete_dir/file.txt", "Delete me")

      :ok =
        Depot.Adapter.Local.create_collection(config, %Depot.Address{path: "/delete_dir/subdir"})

      assert :ok =
               Depot.Adapter.Local.delete_collection(config, %Depot.Address{path: "/delete_dir"},
                 recursive: true
               )

      assert {:ok, :missing} = Depot.Adapter.Local.exists?(config, "/delete_dir")
      assert {:ok, :missing} = Depot.Adapter.Local.exists?(config, "/delete_dir/file.txt")
      assert {:ok, :missing} = Depot.Adapter.Local.exists?(config, "/delete_dir/subdir")

      assert {:error, :enoent} =
               Depot.Adapter.Local.delete_collection(config, %Depot.Address{
                 path: "/non_existent_dir"
               })
    end
  end
end
