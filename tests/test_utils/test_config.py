# -*- coding: utf-8 -*-
"""Tests for kiara configuration utilities."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kiara.exceptions import KiaraException
from kiara.utils.config import assemble_kiara_config


class TestConfigUtilities:
    """Test suite for configuration utility functions."""

    def test_assemble_kiara_config_default(self):
        """Test assembling config with default settings."""
        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.return_value = mock_config_instance
            mock_config_class.load_from_file.return_value = mock_config_instance

            with patch("kiara.utils.config.Path") as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = False
                mock_path.return_value = mock_path_instance

                result = assemble_kiara_config()

                assert result == mock_config_instance
                mock_config_class.assert_called_once()

    def test_assemble_kiara_config_existing_file(self):
        """Test assembling config from existing file."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b"test config content")

        try:
            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.load_from_file.return_value = mock_config_instance

                result = assemble_kiara_config(config_file=temp_path)

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()
        finally:
            os.unlink(temp_path)

    def test_assemble_kiara_config_existing_directory_with_config(self):
        """Test assembling config from directory containing config file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file_path = Path(temp_dir) / "kiara.config"
            config_file_path.write_text("test config")

            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.load_from_file.return_value = mock_config_instance

                result = assemble_kiara_config(config_file=temp_dir)

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()

    def test_assemble_kiara_config_existing_directory_without_config(self):
        """Test assembling config from directory without config file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # This should raise an exception because the directory exists but has no config file
            # and create_config_file is False by default
            with pytest.raises(
                KiaraException, match="specified config file does not exist"
            ):
                assemble_kiara_config(config_file=temp_dir)

    def test_assemble_kiara_config_nonexistent_file_no_create(self):
        """Test assembling config from non-existent file without create flag."""
        # Use a path in a writable directory to avoid permission issues
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_path = Path(temp_dir) / "nonexistent" / "config.file"

            with pytest.raises(
                KiaraException, match="specified config file does not exist"
            ):
                assemble_kiara_config(
                    config_file=str(nonexistent_path), create_config_file=False
                )

    def test_assemble_kiara_config_nonexistent_file_with_create(self):
        """Test assembling config from non-existent file with create flag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "new_config.file"

            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.return_value = mock_config_instance

                result = assemble_kiara_config(
                    config_file=str(config_path), create_config_file=True
                )

                assert result == mock_config_instance
                mock_config_class.assert_called_once()

    def test_assemble_kiara_config_create_parent_directories(self):
        """Test that parent directories are created when needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = Path(temp_dir) / "nested" / "deep" / "config.file"

            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.return_value = mock_config_instance

                result = assemble_kiara_config(
                    config_file=str(nested_path), create_config_file=True
                )

                assert result == mock_config_instance
                # Verify parent directories were created
                assert nested_path.parent.exists()

    def test_assemble_kiara_config_default_with_existing_main_config(self):
        """Test assembling config with existing main config file."""
        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.load_from_file.return_value = mock_config_instance

            with patch("kiara.utils.config.Path") as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = True
                mock_path.return_value = mock_path_instance

                result = assemble_kiara_config()

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()

    def test_assemble_kiara_config_default_create_new(self):
        """Test assembling config with default path and create_config_file=True."""
        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.return_value = mock_config_instance

            with patch("kiara.utils.config.Path") as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = False
                mock_path.return_value = mock_path_instance

                result = assemble_kiara_config(create_config_file=True)

                assert result == mock_config_instance
                mock_config_instance.save.assert_called_once()

    def test_assemble_kiara_config_file_vs_directory_logic(self):
        """Test the logic for distinguishing between files and directories."""
        # Test with file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b"config content")

        try:
            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.load_from_file.return_value = mock_config_instance

                result = assemble_kiara_config(config_file=temp_path)

                assert result == mock_config_instance
                # Should call load_from_file with the actual file path
                mock_config_class.load_from_file.assert_called_once()
                args, kwargs = mock_config_class.load_from_file.call_args
                assert str(args[0]) == temp_path
        finally:
            os.unlink(temp_path)

    def test_assemble_kiara_config_constants_usage(self):
        """Test that the function uses the correct constants."""
        from kiara.defaults import KIARA_CONFIG_FILE_NAME

        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.return_value = mock_config_instance
            mock_config_class.load_from_file.return_value = mock_config_instance

            # Test directory with config file name
            with tempfile.TemporaryDirectory() as temp_dir:
                config_file_path = Path(temp_dir) / KIARA_CONFIG_FILE_NAME
                config_file_path.write_text("test config")

                result = assemble_kiara_config(config_file=temp_dir)

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()
                args, kwargs = mock_config_class.load_from_file.call_args
                assert args[0].name == KIARA_CONFIG_FILE_NAME

    def test_assemble_kiara_config_path_objects(self):
        """Test that the function works with Path objects."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(b"config content")

        try:
            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.load_from_file.return_value = mock_config_instance

                result = assemble_kiara_config(config_file=str(temp_path))

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()
        finally:
            temp_path.unlink()

    def test_assemble_kiara_config_edge_cases(self):
        """Test edge cases and error conditions."""
        # Test with empty string
        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.return_value = mock_config_instance

            with patch("kiara.utils.config.Path") as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = False
                mock_path.return_value = mock_path_instance

                # Empty string should be treated as None
                result = assemble_kiara_config(config_file="")

                assert result == mock_config_instance

    def test_assemble_kiara_config_integration_scenario(self):
        """Test a realistic integration scenario."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a config file
            config_file_path = Path(temp_dir) / "test_config.yaml"
            config_file_path.write_text("version: 1\nname: test_config")

            with patch("kiara.context.KiaraConfig") as mock_config_class:
                mock_config_instance = MagicMock()
                mock_config_class.load_from_file.return_value = mock_config_instance

                # Test loading existing config
                result = assemble_kiara_config(config_file=str(config_file_path))

                assert result == mock_config_instance
                mock_config_class.load_from_file.assert_called_once()

                # Verify the path passed to load_from_file
                args, kwargs = mock_config_class.load_from_file.call_args
                assert Path(args[0]) == config_file_path

    def test_assemble_kiara_config_saves_to_correct_path(self):
        """Test that config is saved to the correct path when create_config_file=True."""
        with patch("kiara.context.KiaraConfig") as mock_config_class:
            mock_config_instance = MagicMock()
            mock_config_class.return_value = mock_config_instance

            with patch("kiara.utils.config.Path") as mock_path:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = False
                mock_path.return_value = mock_path_instance

                result = assemble_kiara_config(create_config_file=True)

                assert result == mock_config_instance
                # Should save to the main config file path
                mock_config_instance.save.assert_called_once()
                args, kwargs = mock_config_instance.save.call_args
                assert args[0] == mock_path_instance
