# -*- coding: utf-8 -*-
import datetime
import os
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path
from typing import List, Union

import pytest

from kiara.defaults import (
    ALL_REQUIRED_TABLES,
    TABLE_NAME_DATA_PEDIGREE,
    TABLE_NAME_DATA_METADATA,
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_DATA_CHUNKS,
    TABLE_NAME_DATA_DESTINIES,
    TABLE_NAME_DATA_SERIALIZATION_METADATA,
    TABLE_NAME_ALIASES,
)
from kiara.interfaces.python_api.base_api import BaseAPI
from kiara.models.values.value import ValueMapReadOnly, Value


# flake8: noqa


def run_sql_query(sql: str, archive_file: Union[str, Path]):
    con = sqlite3.connect(archive_file)
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    con.close()
    return result


def check_archive_contains_table_names(
    archive_file: Union[str, Path], required_tables: List[str]
):

    con = sqlite3.connect(archive_file)

    cursor = con.cursor()
    sql = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(sql)

    tables = {t[0] for t in cursor.fetchall()}
    con.close()

    if not set(required_tables).issubset(tables):
        raise Exception(
            f"Archive file does not contain all required tables: {required_tables} not in {tables}"
        )


def check_table_is_empty(archive_file: Union[str, Path], table_name: str):

    con = sqlite3.connect(archive_file)

    cursor = con.cursor()
    sql = f'SELECT COUNT(*) FROM "{table_name}";'
    cursor.execute(sql)

    count = cursor.fetchone()[0]
    con.close()

    if count > 0:
        raise Exception(f"Table {table_name} is not empty")


def check_tables_are_empty(archive_file: Union[str, Path], *table_names: str):

    for table_name in table_names:
        check_table_is_empty(archive_file, table_name)


def check_table_is_not_empty(archive_file: Union[str, Path], table_name: str):

    con = sqlite3.connect(archive_file)

    cursor = con.cursor()
    sql = f'SELECT COUNT(*) FROM "{table_name}";'
    cursor.execute(sql)

    count = cursor.fetchone()[0]
    con.close()

    if count == 0:
        raise Exception(f"Table {table_name} is empty")


def check_tables_are_not_empty(archive_file: Union[str, Path], *table_names: str):

    for table_name in table_names:
        check_table_is_not_empty(archive_file, table_name)


# TODO: fix for windows
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Does not run on Windows for some reason, need to investigate",
)
def test_archive_export_values_no_alias(api: BaseAPI):

    result: ValueMapReadOnly = api.run_job(
        operation="logic.and", inputs={"a": True, "b": True}
    )

    with tempfile.TemporaryDirectory(suffix="no_alias") as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_no_alias.kiarchive"
        temp_file_path = temp_file_path.resolve()

        store_result = api.export_values(
            temp_file_path, result, alias_map=False, export_related_metadata=False
        )

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.as_posix()} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 1
        assert "y" in store_result.keys()

        required_tables = ALL_REQUIRED_TABLES

        check_archive_contains_table_names(temp_file_path, required_tables)

        check_table_is_empty(temp_file_path, TABLE_NAME_ALIASES)
        check_tables_are_not_empty(
            temp_file_path,
            TABLE_NAME_DATA_PEDIGREE,
            TABLE_NAME_DATA_METADATA,
            TABLE_NAME_ARCHIVE_METADATA,
            TABLE_NAME_DATA_CHUNKS,
            TABLE_NAME_DATA_DESTINIES,
            TABLE_NAME_DATA_SERIALIZATION_METADATA,
        )


# TODO: fix for windows
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Does not run on Windows for some reason, need to investigate",
)
def test_archive_export_values_alias(api: BaseAPI):

    result: ValueMapReadOnly = api.run_job(
        operation="logic.and", inputs={"a": True, "b": True}
    )

    with tempfile.TemporaryDirectory(suffix="alias") as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_alias.kiarchive"
        temp_file_path = temp_file_path.resolve()

        store_result = api.export_values(
            temp_file_path, result, alias_map=True, export_related_metadata=False
        )

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.name} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 1
        assert "y" in store_result.keys()

        required_tables = ALL_REQUIRED_TABLES
        check_archive_contains_table_names(temp_file_path, required_tables)

        check_tables_are_not_empty(
            temp_file_path,
            TABLE_NAME_DATA_PEDIGREE,
            TABLE_NAME_DATA_METADATA,
            TABLE_NAME_ARCHIVE_METADATA,
            TABLE_NAME_DATA_CHUNKS,
            TABLE_NAME_DATA_DESTINIES,
            TABLE_NAME_DATA_SERIALIZATION_METADATA,
            TABLE_NAME_ALIASES,
        )

        result = run_sql_query(f'SELECT * FROM "{TABLE_NAME_ALIASES}";', temp_file_path)
        assert len(result) == 1

        assert result[0][0] == "y"
        assert uuid.UUID(result[0][1])


# TODO: fix for windows
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Does not run on Windows for some reason, need to investigate",
)
def test_archive_export_values_alias_multipe_values(api: BaseAPI):

    result_1: Value = api.run_job(operation="logic.and", inputs={"a": True, "b": True})[
        "y"
    ]
    result_2: Value = api.run_job(
        operation="logic.nand", inputs={"a": True, "b": True}
    )["y"]

    results = {
        "result_1": result_1,
        "result_2": result_2,
    }

    with tempfile.TemporaryDirectory(suffix="multiple") as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_alias_multiple_values.kiarchive"
        temp_file_path = temp_file_path.resolve()

        store_result = api.export_values(
            temp_file_path, results, alias_map=True, export_related_metadata=False
        )

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.name} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 2
        assert "result_1" in store_result.keys()
        assert "result_2" in store_result.keys()

        required_tables = ALL_REQUIRED_TABLES
        check_archive_contains_table_names(temp_file_path, required_tables)

        check_tables_are_not_empty(
            temp_file_path,
            TABLE_NAME_DATA_PEDIGREE,
            TABLE_NAME_DATA_METADATA,
            TABLE_NAME_ARCHIVE_METADATA,
            TABLE_NAME_DATA_CHUNKS,
            TABLE_NAME_DATA_DESTINIES,
            TABLE_NAME_DATA_SERIALIZATION_METADATA,
            TABLE_NAME_ALIASES,
        )

        result = run_sql_query(f'SELECT * FROM "{TABLE_NAME_ALIASES}";', temp_file_path)

        print(result)
        assert len(result) == 2
        assert len(result[0]) == 3
        assert result[0][0] in ["result_1", "result_2"]
        assert uuid.UUID(result[0][1])
        datetime.datetime.fromisoformat(result[0][2])

        assert len(result[1]) == 3
        assert result[1][0] in ["result_1", "result_2"]
        assert uuid.UUID(result[1][1])
        datetime.datetime.fromisoformat(result[1][2])
