# -*- coding: utf-8 -*-
"""Tests for kiara file utilities."""

import json
import os
import tempfile
import zipfile
from pathlib import Path

import pytest
from ruamel.yaml import YAML

from kiara.exceptions import KiaraException
from kiara.utils.files import get_data_from_file, unpack_archive


class TestFileUtilities:
    """Test suite for file utility functions."""

    def test_get_data_from_json_file(self):
        """Test reading JSON data from file."""
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_yaml_file(self):
        """Test reading YAML data from file."""
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml = YAML()
            yaml.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_yml_file(self):
        """Test reading YAML data from .yml file."""
        data = {"test": "data"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml = YAML()
            yaml.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_with_explicit_content_type(self):
        """Test reading data with explicit content type."""
        json_data = {"type": "json"}
        yaml_data = {"type": "yaml"}

        # Test JSON with no extension
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            json.dump(json_data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file, content_type="json")
            assert result == json_data
        finally:
            os.unlink(temp_file)

        # Test YAML with no extension
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            yaml = YAML()
            yaml.dump(yaml_data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file, content_type="yaml")
            assert result == yaml_data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_autodetect_json(self):
        """Test autodetecting JSON format without extension."""
        data = {"key": "value"}

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            json.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_autodetect_yaml(self):
        """Test autodetecting YAML format without extension."""
        # Use simple YAML that won't be valid JSON
        yaml_content = "key: value\nnumber: 42\n"

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == {"key": "value", "number": 42}
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_with_path_object(self):
        """Test reading data using Path object."""
        data = {"test": "data"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            temp_file = Path(f.name)

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            temp_file.unlink()

    def test_get_data_from_file_with_home_expansion(self):
        """Test reading data with home directory expansion."""
        data = {"test": "data"}

        # Create a temp file in home directory
        home = Path.home()
        temp_file = home / f".test_kiara_{os.getpid()}.json"

        try:
            with open(temp_file, "w") as f:
                json.dump(data, f)

            # Use ~ notation
            result = get_data_from_file(f"~/{temp_file.name}")
            assert result == data
        finally:
            if temp_file.exists():
                temp_file.unlink()

    def test_get_data_from_file_nonexistent(self):
        """Test reading from non-existent file."""
        with pytest.raises(KiaraException, match="File not found"):
            get_data_from_file("/nonexistent/file.json")

    def test_get_data_from_file_directory(self):
        """Test reading from directory instead of file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(KiaraException, match="Path is not a file"):
                get_data_from_file(temp_dir)

    def test_get_data_from_file_invalid_content_type(self):
        """Test with invalid content type."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test")
            temp_file = f.name

        try:
            with pytest.raises(KiaraException, match="Invalid content type"):
                get_data_from_file(temp_file, content_type="xml")
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_invalid_format(self):
        """Test with file that's neither JSON nor YAML."""
        # YAML can parse plain text as a string, so we need something that fails both
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            # Write invalid YAML that also isn't JSON
            f.write("{ invalid json\n- invalid yaml :")
            temp_file = f.name

        try:
            with pytest.raises(ValueError, match="Could not determine data format"):
                get_data_from_file(temp_file)
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_empty(self):
        """Test reading empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_file = f.name

        try:
            with pytest.raises(Exception):  # JSON/YAML parsing will fail
                get_data_from_file(temp_file)
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_complex_data(self):
        """Test reading complex nested data structures."""
        data = {
            "nested": {"deep": {"value": 42, "list": [1, 2, {"inner": "value"}]}},
            "array": [None, True, False, 3.14],
            "unicode": "Hello 世界 🌍",
        }

        # Test with JSON
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

        # Test with YAML
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml = YAML()
            yaml.dump(data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_unpack_archive_zip(self):
        """Test unpacking ZIP archive."""
        # Create a ZIP file with test content
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "test.zip")
            extract_dir = os.path.join(temp_dir, "extracted")

            # Create ZIP with test files
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("file1.txt", "Content of file 1")
                zf.writestr("dir/file2.txt", "Content of file 2")

            # Extract archive
            unpack_archive(zip_path, extract_dir)

            # Verify extraction
            assert os.path.exists(os.path.join(extract_dir, "file1.txt"))
            assert os.path.exists(os.path.join(extract_dir, "dir", "file2.txt"))

            with open(os.path.join(extract_dir, "file1.txt")) as f:
                assert f.read() == "Content of file 1"

    def test_unpack_archive_tar_gz(self):
        """Test unpacking tar.gz archive."""
        import tarfile

        with tempfile.TemporaryDirectory() as temp_dir:
            tar_path = os.path.join(temp_dir, "test.tar.gz")
            extract_dir = os.path.join(temp_dir, "extracted")

            # Create tar.gz with test files
            with tarfile.open(tar_path, "w:gz") as tar:
                # Create temporary files to add
                file1 = os.path.join(temp_dir, "file1.txt")
                with open(file1, "w") as f:
                    f.write("Content 1")
                tar.add(file1, arcname="file1.txt")

            # Extract archive
            unpack_archive(tar_path, extract_dir)

            # Verify extraction
            assert os.path.exists(os.path.join(extract_dir, "file1.txt"))

    def test_unpack_archive_nonexistent(self):
        """Test unpacking non-existent archive."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(Exception):  # Will raise some exception
                unpack_archive("/nonexistent/archive.zip", temp_dir)

    def test_unpack_archive_invalid_format(self):
        """Test unpacking invalid archive format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a non-archive file
            invalid_file = os.path.join(temp_dir, "not_an_archive.txt")
            with open(invalid_file, "w") as f:
                f.write("This is not an archive")

            extract_dir = os.path.join(temp_dir, "extracted")

            # Don't use autodetect_file_type=True since it's not implemented
            with pytest.raises(Exception):  # Will raise some exception during unpacking
                unpack_archive(invalid_file, extract_dir)

    def test_unpack_archive_autodetect_not_implemented(self):
        """Test that autodetect raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Autodetecting file type"):
            unpack_archive("dummy.file", "dummy_dir", autodetect_file_type=True)

    def test_get_data_from_file_with_encoding_issues(self):
        """Test reading files with different encodings."""
        # This tests that the default read_text() works with UTF-8
        data = {"text": "Hello 世界"}

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".json", delete=False
        ) as f:
            json.dump(data, f, ensure_ascii=False)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == data
        finally:
            os.unlink(temp_file)

    def test_get_data_from_file_large(self):
        """Test reading large files."""
        # Create a large data structure
        large_data = {
            f"key_{i}": {"value": i, "data": list(range(100))} for i in range(1000)
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(large_data, f)
            temp_file = f.name

        try:
            result = get_data_from_file(temp_file)
            assert result == large_data
            assert len(result) == 1000
        finally:
            os.unlink(temp_file)
