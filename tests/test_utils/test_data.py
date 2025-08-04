# -*- coding: utf-8 -*-
"""Tests for kiara data manipulation utilities."""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from kiara.utils.data import get_data_from_string, pretty_print_data


class TestDataUtilities:
    """Test suite for data utility functions."""

    def test_get_data_from_string_json(self):
        """Test parsing JSON data from string."""
        json_string = '{"key": "value", "number": 42, "list": [1, 2, 3]}'
        result = get_data_from_string(json_string, content_type="json")

        assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}

    def test_get_data_from_string_yaml(self):
        """Test parsing YAML data from string."""
        yaml_string = """
key: value
number: 42
list:
  - 1
  - 2
  - 3
"""
        result = get_data_from_string(yaml_string, content_type="yaml")

        assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}

    def test_get_data_from_string_autodetect_json(self):
        """Test autodetecting JSON format."""
        json_string = '{"test": "data"}'
        result = get_data_from_string(json_string)

        assert result == {"test": "data"}

    def test_get_data_from_string_autodetect_yaml(self):
        """Test autodetecting YAML format."""
        # Use YAML that's not valid JSON
        yaml_string = "key: value\nnumber: 42"
        result = get_data_from_string(yaml_string)

        assert result == {"key": "value", "number": 42}

    def test_get_data_from_string_invalid_content_type(self):
        """Test with invalid content type."""
        with pytest.raises(AssertionError):
            get_data_from_string("test", content_type="xml")

    def test_get_data_from_string_invalid_format(self):
        """Test with string that's neither JSON nor YAML."""
        # Use invalid syntax for both formats
        invalid_string = "{ invalid json\n- invalid yaml :"

        with pytest.raises(ValueError, match="Invalid data format"):
            get_data_from_string(invalid_string)

    def test_get_data_from_string_empty(self):
        """Test parsing empty string."""
        # Empty string parses as None in YAML
        result = get_data_from_string("")
        assert result is None

    def test_get_data_from_string_complex_json(self):
        """Test parsing complex JSON structures."""
        json_string = """
        {
            "nested": {
                "deep": {
                    "value": 42,
                    "list": [1, 2, {"inner": "value"}]
                }
            },
            "array": [null, true, false, 3.14],
            "unicode": "Hello 世界 🌍"
        }
        """
        result = get_data_from_string(json_string, content_type="json")

        assert result["nested"]["deep"]["value"] == 42
        assert result["array"] == [None, True, False, 3.14]
        assert result["unicode"] == "Hello 世界 🌍"

    def test_get_data_from_string_complex_yaml(self):
        """Test parsing complex YAML structures."""
        yaml_string = """
nested:
  deep:
    value: 42
    list:
      - 1
      - 2
      - inner: value
array:
  - null
  - true
  - false
  - 3.14
unicode: Hello 世界 🌍
"""
        result = get_data_from_string(yaml_string, content_type="yaml")

        assert result["nested"]["deep"]["value"] == 42
        assert result["array"] == [None, True, False, 3.14]
        assert result["unicode"] == "Hello 世界 🌍"

    def test_get_data_from_string_json_whitespace(self):
        """Test JSON parsing with various whitespace."""
        json_string = '\n\t  {"key":\n\t"value"}\n\t  '
        result = get_data_from_string(json_string)

        assert result == {"key": "value"}

    def test_get_data_from_string_yaml_multiline(self):
        """Test YAML multiline string parsing."""
        yaml_string = """
description: |
  This is a
  multiline string
  in YAML
key: value
"""
        result = get_data_from_string(yaml_string, content_type="yaml")

        assert "This is a\nmultiline string\nin YAML\n" == result["description"]
        assert result["key"] == "value"

    def test_get_data_from_string_json_special_chars(self):
        """Test JSON with special characters."""
        json_string = '{"path": "/home/user", "regex": "\\\\d+", "quote": "\\"test\\""}'
        result = get_data_from_string(json_string, content_type="json")

        assert result["path"] == "/home/user"
        assert result["regex"] == "\\d+"
        assert result["quote"] == '"test"'

    def test_get_data_from_string_yaml_special_types(self):
        """Test YAML with special types."""
        yaml_string = """
date: 2023-01-01
float: 3.14159
scientific: 1.23e-4
infinity: .inf
not_a_number: .nan
"""
        result = get_data_from_string(yaml_string, content_type="yaml")

        assert result["float"] == 3.14159
        assert result["scientific"] == 0.000123
        # YAML safe mode might handle these differently
        assert "infinity" in result
        assert "not_a_number" in result

    @patch("kiara.utils.data.orjson.loads")
    @patch("kiara.utils.data.yaml.load")
    def test_get_data_from_string_fallback(self, mock_yaml_load, mock_json_loads):
        """Test fallback from JSON to YAML parsing."""
        test_string = "test data"

        # Make JSON parsing fail, YAML succeed
        mock_json_loads.side_effect = Exception("JSON parse error")
        mock_yaml_load.return_value = {"parsed": "yaml"}

        result = get_data_from_string(test_string)

        assert result == {"parsed": "yaml"}
        mock_json_loads.assert_called_once()
        mock_yaml_load.assert_called_once()

    def test_pretty_print_data_basic(self):
        """Test pretty_print_data with mocked Kiara context."""
        # Create mocks
        mock_kiara = MagicMock()
        mock_value = MagicMock()
        mock_value.data_type_name = "string"
        mock_value.value_schema.type = "string"

        mock_kiara.data_registry.get_value.return_value = mock_value
        mock_kiara.data_type_names = ["string", "integer", "boolean"]

        mock_op_type = MagicMock()
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.get_value_data.return_value = "Rendered output"

        mock_op.run.return_value = mock_result
        mock_op_type.get_operation_for_render_combination.return_value = mock_op
        mock_kiara.operation_registry.get_operation_type.return_value = mock_op_type

        # Test the function
        test_uuid = uuid.uuid4()
        result = pretty_print_data(mock_kiara, test_uuid)

        assert result == "Rendered output"
        mock_kiara.data_registry.get_value.assert_called_once_with(value=test_uuid)
        mock_op_type.get_operation_for_render_combination.assert_called_once_with(
            source_type="string", target_type="terminal_renderable"
        )

    def test_pretty_print_data_unknown_type(self):
        """Test pretty_print_data with unknown data type."""
        # Create mocks
        mock_kiara = MagicMock()
        mock_value = MagicMock()
        mock_value.data_type_name = "unknown_type"
        mock_value.value_schema.type = "unknown_type"

        mock_kiara.data_registry.get_value.return_value = mock_value
        mock_kiara.data_type_names = ["string", "integer", "boolean"]

        mock_op_type = MagicMock()
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.get_value_data.return_value = "Rendered as any"

        mock_op.run.return_value = mock_result
        mock_op_type.get_operation_for_render_combination.return_value = mock_op
        mock_kiara.operation_registry.get_operation_type.return_value = mock_op_type

        # Test the function
        test_uuid = uuid.uuid4()
        result = pretty_print_data(mock_kiara, test_uuid)

        assert result == "Rendered as any"
        # Should use "any" for unknown types
        mock_op_type.get_operation_for_render_combination.assert_called_with(
            source_type="any", target_type="terminal_renderable"
        )

    def test_pretty_print_data_with_render_config(self):
        """Test pretty_print_data with custom render configuration."""
        # Create mocks
        mock_kiara = MagicMock()
        mock_value = MagicMock()
        mock_value.data_type_name = "table"
        mock_value.value_schema.type = "table"

        mock_kiara.data_registry.get_value.return_value = mock_value
        mock_kiara.data_type_names = ["table", "string"]

        mock_op_type = MagicMock()
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.get_value_data.return_value = "Rendered table"

        mock_op.run.return_value = mock_result
        mock_op_type.get_operation_for_render_combination.return_value = mock_op
        mock_kiara.operation_registry.get_operation_type.return_value = mock_op_type

        # Test with custom render config
        test_uuid = uuid.uuid4()
        render_config = {"max_rows": 10, "show_header": True}
        result = pretty_print_data(
            mock_kiara, test_uuid, target_type="html", **render_config
        )

        assert result == "Rendered table"
        mock_op.run.assert_called_once_with(
            kiara=mock_kiara,
            inputs={"value": mock_value, "render_config": render_config},
        )

    def test_pretty_print_data_no_operation_found(self):
        """Test pretty_print_data when no operation is found."""
        # Create mocks
        mock_kiara = MagicMock()
        mock_value = MagicMock()
        mock_value.data_type_name = "custom_type"
        mock_value.value_schema.type = "custom_type"

        mock_kiara.data_registry.get_value.return_value = mock_value
        mock_kiara.data_type_names = ["string"]

        mock_op_type = MagicMock()
        # Make all attempts to get operation fail
        mock_op_type.get_operation_for_render_combination.side_effect = Exception(
            "Not found"
        )
        mock_kiara.operation_registry.get_operation_type.return_value = mock_op_type

        # Test the function
        test_uuid = uuid.uuid4()
        with pytest.raises(Exception, match="Can't find operation to render"):
            pretty_print_data(mock_kiara, test_uuid)

    def test_pretty_print_data_fallback_to_string(self):
        """Test pretty_print_data fallback to string rendering."""
        # Create mocks
        mock_kiara = MagicMock()
        mock_value = MagicMock()
        mock_value.data_type_name = "complex_type"
        mock_value.value_schema.type = "complex_type"

        mock_kiara.data_registry.get_value.return_value = mock_value
        mock_kiara.data_type_names = ["string"]

        mock_op_type = MagicMock()
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.get_value_data.return_value = "Fallback string render"

        mock_op.run.return_value = mock_result

        # First call fails, second succeeds (fallback)
        mock_op_type.get_operation_for_render_combination.side_effect = [
            Exception("Not found"),
            mock_op,
        ]
        mock_kiara.operation_registry.get_operation_type.return_value = mock_op_type

        # Test the function
        test_uuid = uuid.uuid4()
        result = pretty_print_data(
            mock_kiara, test_uuid, target_type="terminal_renderable"
        )

        assert result == "Fallback string render"
        # Should have tried twice
        assert mock_op_type.get_operation_for_render_combination.call_count == 2

    def test_get_data_from_string_large_json(self):
        """Test parsing large JSON data."""
        # Create large JSON structure
        large_data = {
            f"key_{i}": {"value": i, "data": list(range(100))} for i in range(100)
        }

        import json

        json_string = json.dumps(large_data)
        result = get_data_from_string(json_string, content_type="json")

        assert len(result) == 100
        assert result["key_50"]["value"] == 50

    def test_get_data_from_string_numeric_types(self):
        """Test parsing various numeric types."""
        json_string = """
        {
            "integer": 42,
            "float": 3.14,
            "scientific": 1.23e10,
            "negative": -100,
            "zero": 0
        }
        """
        result = get_data_from_string(json_string)

        assert result["integer"] == 42
        assert result["float"] == 3.14
        assert result["scientific"] == 1.23e10
        assert result["negative"] == -100
        assert result["zero"] == 0
