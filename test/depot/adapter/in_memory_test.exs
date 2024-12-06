defmodule Depot.Adapter.InMemoryTest do
  use ExUnit.Case, async: true
  import Depot.AdapterTest
  doctest Depot.Adapter.InMemory

  adapter_test %{test: test} do
    {:ok, filesystem} = Depot.Adapter.InMemory.start(name: test)
    {:ok, filesystem: filesystem}
  end

  describe "basic adapter methods" do
    setup %{test: test} do
      {:ok, config} = Depot.Adapter.InMemory.start(name: test)
      {:ok, config: config}
    end

    test "read", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/test.txt", "Hello World")
      assert {:ok, "Hello World"} = Depot.Adapter.InMemory.read(config, "/test.txt")
      assert {:error, :enoent} = Depot.Adapter.InMemory.read(config, "/non_existent.txt")
    end

    test "write", %{config: config} do
      assert :ok = Depot.Adapter.InMemory.write(config, "/new_file.txt", "New content")
      assert {:ok, "New content"} = Depot.Adapter.InMemory.read(config, "/new_file.txt")
      assert :ok = Depot.Adapter.InMemory.write(config, "/new_file.txt", "Updated content")
      assert {:ok, "Updated content"} = Depot.Adapter.InMemory.read(config, "/new_file.txt")
      assert :ok = Depot.Adapter.InMemory.write(config, "/nested/path/file.txt", "Nested content")

      assert {:ok, "Nested content"} =
               Depot.Adapter.InMemory.read(config, "/nested/path/file.txt")
    end

    test "delete", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/to_delete.txt", "Delete me")
      assert :ok = Depot.Adapter.InMemory.delete(config, "/to_delete.txt")
      assert {:error, :enoent} = Depot.Adapter.InMemory.read(config, "/to_delete.txt")
      assert :ok = Depot.Adapter.InMemory.delete(config, "/non_existent.txt")
    end

    test "move", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/source.txt", "Move me")
      assert :ok = Depot.Adapter.InMemory.move(config, "/source.txt", "/destination.txt", [])
      assert {:error, :enoent} = Depot.Adapter.InMemory.read(config, "/source.txt")
      assert {:ok, "Move me"} = Depot.Adapter.InMemory.read(config, "/destination.txt")

      assert {:error, :enoent} =
               Depot.Adapter.InMemory.move(config, "/non_existent.txt", "/new_place.txt", [])
    end

    test "copy", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/original.txt", "Copy me")
      assert :ok = Depot.Adapter.InMemory.copy(config, "/original.txt", "/copy.txt", [])
      assert {:ok, "Copy me"} = Depot.Adapter.InMemory.read(config, "/original.txt")
      assert {:ok, "Copy me"} = Depot.Adapter.InMemory.read(config, "/copy.txt")

      assert {:error, :enoent} =
               Depot.Adapter.InMemory.copy(config, "/non_existent.txt", "/new_copy.txt", [])
    end

    test "exists?", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/exists.txt", "I exist")
      assert {:ok, :exists} = Depot.Adapter.InMemory.exists?(config, "/exists.txt")
      assert {:ok, :missing} = Depot.Adapter.InMemory.exists?(config, "/does_not_exist.txt")
    end
  end

  describe "stream operations" do
    setup %{test: test} do
      {:ok, config} = Depot.Adapter.InMemory.start(name: test)
      {:ok, config: config}
    end

    test "read_stream", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/stream_read.txt", "Line 1\nLine 2\nLine 3")

      {:ok, stream} =
        Depot.Adapter.InMemory.read_stream(config, %Depot.Address{path: "/stream_read.txt"}, [])

      # The entire content is returned as a single chunk when no chunk_size is specified
      assert Enum.to_list(stream) == ["Line 1\nLine 2\nLine 3"]

      {:ok, custom_stream} =
        Depot.Adapter.InMemory.read_stream(config, %Depot.Address{path: "/stream_read.txt"},
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

      assert {:error, :enoent} =
               Depot.Adapter.InMemory.read_stream(
                 config,
                 %Depot.Address{path: "/non_existent.txt"},
                 []
               )
    end

    test "write_stream", %{config: config} do
      stream = Stream.map(1..3, &"Line #{&1}\n")

      {:ok, file_stream} =
        Depot.Adapter.InMemory.write_stream(config, %Depot.Address{path: "/stream_write.txt"}, [])

      Enum.into(stream, file_stream)

      assert {:ok, "Line 1\nLine 2\nLine 3\n"} =
               Depot.Adapter.InMemory.read(config, "/stream_write.txt")

      custom_stream = Stream.map(1..3, &"Custom #{&1}\n")

      {:ok, custom_file_stream} =
        Depot.Adapter.InMemory.write_stream(
          config,
          %Depot.Address{path: "/custom_stream_write.txt"},
          chunk_size: 2
        )

      Enum.into(custom_stream, custom_file_stream)

      assert {:ok, "Custom 1\nCustom 2\nCustom 3\n"} =
               Depot.Adapter.InMemory.read(config, "/custom_stream_write.txt")
    end
  end

  describe "collection operations" do
    setup %{test: test} do
      {:ok, config} = Depot.Adapter.InMemory.start(name: test)
      {:ok, config: config}
    end

    test "list", %{config: config} do
      :ok = Depot.Adapter.InMemory.write(config, "/list_test/file1.txt", "Content 1")
      :ok = Depot.Adapter.InMemory.write(config, "/list_test/file2.txt", "Content 2")

      :ok =
        Depot.Adapter.InMemory.create_collection(config, %Depot.Address{path: "/list_test/subdir"})

      {:ok, entries} = Depot.Adapter.InMemory.list(config, %Depot.Address{path: "/list_test"})
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

      assert {:ok, []} =
               Depot.Adapter.InMemory.list(config, %Depot.Address{path: "/non_existent_dir"})
    end

    test "create_collection", %{config: config} do
      assert :ok =
               Depot.Adapter.InMemory.create_collection(config, %Depot.Address{path: "/new_dir"})

      assert {:ok, :exists} = Depot.Adapter.InMemory.exists?(config, "/new_dir")

      assert :ok =
               Depot.Adapter.InMemory.create_collection(config, %Depot.Address{path: "/new_dir"})

      assert :ok =
               Depot.Adapter.InMemory.create_collection(config, %Depot.Address{
                 path: "/parent/child/grandchild"
               })

      assert {:ok, :exists} = Depot.Adapter.InMemory.exists?(config, "/parent/child/grandchild")
    end

    test "delete_collection", %{config: config} do
      :ok = Depot.Adapter.InMemory.create_collection(config, %Depot.Address{path: "/delete_dir"})
      :ok = Depot.Adapter.InMemory.write(config, "/delete_dir/file.txt", "Delete me")

      :ok =
        Depot.Adapter.InMemory.create_collection(config, %Depot.Address{
          path: "/delete_dir/subdir"
        })

      assert :ok =
               Depot.Adapter.InMemory.delete_collection(
                 config,
                 %Depot.Address{path: "/delete_dir"},
                 recursive: true
               )

      assert {:ok, :missing} = Depot.Adapter.InMemory.exists?(config, "/delete_dir")
      assert {:ok, :missing} = Depot.Adapter.InMemory.exists?(config, "/delete_dir/file.txt")
      assert {:ok, :missing} = Depot.Adapter.InMemory.exists?(config, "/delete_dir/subdir")

      assert {:error, :enoent} =
               Depot.Adapter.InMemory.delete_collection(config, %Depot.Address{
                 path: "/non_existent_dir"
               })
    end
  end
end
