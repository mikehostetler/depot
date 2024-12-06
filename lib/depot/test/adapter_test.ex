defmodule Depot.AdapterTest do
  defmacro in_list(list, match) do
    quote do
      Enum.any?(unquote(list), &match?(unquote(match), &1))
    end
  end

  defp tests do
    quote do
      test "user can write to filesystem", %{filesystem: filesystem} do
        assert :ok = Depot.write(filesystem, "/test.txt", "Hello World")
      end

      test "user can read from filesystem", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")

        assert {:ok, "Hello World"} = Depot.read(filesystem, "/test.txt")
      end

      test "user can overwrite a file on the filesystem", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Old text")
        assert :ok = Depot.write(filesystem, "/test.txt", "Hello World")
        assert {:ok, "Hello World"} = Depot.read(filesystem, "/test.txt")
      end

      test "user can check if files exist on a filesystem", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")

        assert {:ok, :exists} = Depot.exists?(filesystem, "/test.txt")
        assert {:ok, :missing} = Depot.exists?(filesystem, "/not-test.txt")
      end

      test "user can stream to a filesystem", %{filesystem: filesystem} do
        case Depot.write_stream(filesystem, "/test.txt") do
          {:ok, stream} ->
            Enum.into(["Hello", " ", "World"], stream)

            case Depot.read(filesystem, "/test.txt") do
              {:ok, content} ->
                assert content == "Hello World"

              error ->
                flunk("Failed to read file after streaming")
            end

          {:error, _} ->
            :ok

          error ->
            flunk("Unexpected error occurred")
        end
      end

      test "user can stream from filesystem", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")

        case Depot.read_stream(filesystem, "/test.txt") do
          {:ok, stream} ->
            assert Enum.into(stream, <<>>) == "Hello World"

          {:error, _} ->
            :ok
        end
      end

      test "user can stream in a certain chunk size", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")

        case Depot.read_stream(filesystem, "/test.txt", chunk_size: 2) do
          {:ok, stream} ->
            assert ["He" | _] = Enum.into(stream, [])

          {:error, _} ->
            :ok
        end
      end

      test "user can try to read a non-existing file from filesystem", %{filesystem: filesystem} do
        assert {:error, :enoent} = Depot.read(filesystem, "/test.txt")
      end

      test "user can delete from filesystem", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")
        :ok = Depot.delete(filesystem, "/test.txt")

        assert {:error, _} = Depot.read(filesystem, "/test.txt")
      end

      test "user can delete a non-existing file from filesystem", %{filesystem: filesystem} do
        assert :ok = Depot.delete(filesystem, "/test.txt")
      end

      test "user can move files", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")
        :ok = Depot.move(filesystem, "/test.txt", "/not-test.txt")

        assert {:error, _} = Depot.read(filesystem, "/test.txt")
        assert {:ok, "Hello World"} = Depot.read(filesystem, "/not-test.txt")
      end

      test "user can try to move a non-existing file", %{filesystem: filesystem} do
        assert {:error, :enoent} = Depot.move(filesystem, "/test.txt", "/not-test.txt")
      end

      test "user can copy files", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test.txt", "Hello World")
        :ok = Depot.copy(filesystem, "/test.txt", "/not-test.txt")

        assert {:ok, "Hello World"} = Depot.read(filesystem, "/test.txt")
        assert {:ok, "Hello World"} = Depot.read(filesystem, "/not-test.txt")
      end

      test "user can try to copy a non-existing file", %{filesystem: filesystem} do
        assert {:error, :enoent} = Depot.copy(filesystem, "/test.txt", "/not-test.txt")
      end

      # test "user can list files and folders", %{filesystem: filesystem} do
      #   :ok = Depot.create_collection(filesystem, "test/")
      #   :ok = Depot.write(filesystem, "test.txt", "Hello World")
      #   :ok = Depot.write(filesystem, "/test-1.txt", "Hello World")
      #   :ok = Depot.write(filesystem, "/folder/test-1.txt", "Hello World")

      #   {:ok, list} = Depot.list(filesystem, ".")
      #   IO.inspect(list, label: "List")

      #   assert in_list(list, %Depot.Resource{address: %{path: "/test"}, type: :directory})
      #   assert in_list(list, %Depot.Resource{address: %{path: "/folder"}, type: :directory})
      #   assert in_list(list, %Depot.Resource{address: %{path: "/test.txt"}, type: :file})
      #   assert in_list(list, %Depot.Resource{address: %{path: "/test-1.txt"}, type: :file})

      #   refute in_list(list, %Depot.Resource{address: %{path: "/folder/test-1.txt"}, type: :file})

      #   assert length(list) == 4
      # end

      test "directory listings include visibility", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/visible.txt", "Hello World", visibility: :public)
        :ok = Depot.write(filesystem, "/invisible.txt", "Hello World", visibility: :private)
        :ok = Depot.create_collection(filesystem, "/visible-dir/", directory_visibility: :public)

        :ok =
          Depot.create_collection(filesystem, "/invisible-dir/", directory_visibility: :private)

        {:ok, list} = Depot.list(filesystem, ".")

        assert in_list(list, %Depot.Resource{
                 address: %{path: "/visible-dir"},
                 type: :directory,
                 metadata: %{visibility: :public}
               })

        assert in_list(list, %Depot.Resource{
                 address: %{path: "/invisible-dir"},
                 type: :directory,
                 metadata: %{visibility: :private}
               })

        assert in_list(list, %Depot.Resource{
                 address: %{path: "/visible.txt"},
                 type: :file,
                 metadata: %{visibility: :public}
               })

        assert in_list(list, %Depot.Resource{
                 address: %{path: "/invisible.txt"},
                 type: :file,
                 metadata: %{visibility: :private}
               })

        assert length(list) == 4
      end

      test "user can create directories", %{filesystem: filesystem} do
        assert :ok = Depot.create_collection(filesystem, "/test/")
        assert :ok = Depot.create_collection(filesystem, "/test/nested/folder/")
      end

      test "user can delete directories", %{filesystem: filesystem} do
        :ok = Depot.create_collection(filesystem, "/test/")
        assert :ok = Depot.delete_collection(filesystem, "/test/")
      end

      test "non empty directories are not deleted by default", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "/test/test.txt", "Hello World")
        assert {:error, _} = Depot.delete_collection(filesystem, "/test/")
      end

      test "non empty directories are deleted with the recursive flag set", %{
        filesystem: filesystem
      } do
        :ok = Depot.write(filesystem, "/test/test.txt", "Hello World")
        assert :ok = Depot.delete_collection(filesystem, "/test/", recursive: true)

        :ok = Depot.create_collection(filesystem, "/test/nested/folder/")
        assert :ok = Depot.delete_collection(filesystem, "/test/", recursive: true)
      end

      test "files in deleted directories are no longer available", %{filesystem: filesystem} do
        :ok = Depot.write(filesystem, "test/test.txt", "Hello World")
        assert :ok = Depot.delete_collection(filesystem, "/test/", recursive: true)
        assert {:ok, :missing} = Depot.exists?(filesystem, "/not-test.txt")
      end

      test "get visibility", %{filesystem: filesystem} do
        :ok =
          Depot.write(filesystem, "/public/file.txt", "Hello World",
            visibility: :private,
            directory_visibility: :public
          )

        :ok =
          Depot.write(filesystem, "/private/file.txt", "Hello World",
            visibility: :public,
            directory_visibility: :private
          )

        assert {:ok, :public} = Depot.get_visibility(filesystem, ".")
        assert {:ok, :public} = Depot.get_visibility(filesystem, "/public/")
        assert {:ok, :private} = Depot.get_visibility(filesystem, "/public/file.txt")
        assert {:ok, :private} = Depot.get_visibility(filesystem, "/private/")
        assert {:ok, :public} = Depot.get_visibility(filesystem, "/private/file.txt")
      end

      test "update visibility", %{filesystem: filesystem} do
        :ok =
          Depot.write(filesystem, "/folder/file.txt", "Hello World",
            visibility: :public,
            directory_visibility: :public
          )

        assert :ok = Depot.set_visibility(filesystem, "/folder/", :private)
        assert {:ok, :private} = Depot.get_visibility(filesystem, "/folder/")

        assert :ok = Depot.set_visibility(filesystem, "/folder/file.txt", :private)
        assert {:ok, :private} = Depot.get_visibility(filesystem, "/folder/file.txt")
      end
    end
  end

  defmacro adapter_test(block) do
    quote do
      describe "common adapter tests" do
        setup unquote(block)

        import Depot.AdapterTest, only: [in_list: 2]
        unquote(tests())
      end
    end
  end

  defmacro adapter_test(context, block) do
    quote do
      describe "common adapter tests" do
        setup unquote(context), unquote(block)

        import Depot.AdapterTest, only: [in_list: 2]
        unquote(tests())
      end
    end
  end
end
