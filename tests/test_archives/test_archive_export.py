# -*- coding: utf-8 -*-
import os
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import List, Union

from kiara.api import KiaraAPI
from kiara.models.values.value import ValueMapReadOnly, Value


# flake8: noqa


def run_sql_query(sql: str, archive_file: str):
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


def test_archive_export_values_no_alias(api: KiaraAPI):

    result: ValueMapReadOnly = api.run_job(
        operation="logic.and", inputs={"a": True, "b": True}
    )

    with tempfile.TemporaryDirectory() as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_no_alias.kiarchive"
        print("temp_file_path", temp_file_path.as_posix())

        store_result = api.export_values(temp_file_path, result, alias_map=False)

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.as_posix()} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 1
        assert "y" in store_result.keys()

        required_tables = [
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "aliases",
            "values_data",
            "values_destinies",
            "persisted_values",
        ]
        check_archive_contains_table_names(temp_file_path, required_tables)

        check_table_is_empty(temp_file_path, "aliases")

        check_tables_are_not_empty(
            temp_file_path,
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "values_data",
            "values_destinies",
            "persisted_values",
        )


def test_archive_export_values_alias(api: KiaraAPI):

    result: ValueMapReadOnly = api.run_job(
        operation="logic.and", inputs={"a": True, "b": True}
    )

    with tempfile.TemporaryDirectory() as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_alias.kiarchive"
        print("temp_file_path", temp_file_path.as_posix())

        store_result = api.export_values(temp_file_path, result, alias_map=True)

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.name} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 1
        assert "y" in store_result.keys()

        required_tables = [
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "aliases",
            "values_data",
            "values_destinies",
            "persisted_values",
        ]
        check_archive_contains_table_names(temp_file_path, required_tables)

        check_tables_are_not_empty(
            temp_file_path,
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "values_data",
            "values_destinies",
            "persisted_values",
            "aliases",
        )

        result = run_sql_query('SELECT * FROM "aliases";', temp_file_path)
        assert len(result) == 1
        assert len(result[0]) == 2
        assert result[0][0] == "y"
        assert uuid.UUID(result[0][1])


def test_archive_export_values_alias_multipe_values(api: KiaraAPI):

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

    with tempfile.TemporaryDirectory() as temp_dir:

        temp_file_path = Path(temp_dir) / "export_test_alias_multiple_values.kiarchive"
        print("temp_file_path", temp_file_path.as_posix())

        store_result = api.export_values(temp_file_path, results, alias_map=True)

        if not temp_file_path.is_file():
            raise Exception(f"Export file {temp_file_path.name} was not created")

        assert temp_file_path.stat().st_size > 0

        assert len(store_result) == 2
        assert "result_1" in store_result.keys()
        assert "result_2" in store_result.keys()

        required_tables = [
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "aliases",
            "values_data",
            "values_destinies",
            "persisted_values",
        ]
        check_archive_contains_table_names(temp_file_path, required_tables)

        check_tables_are_not_empty(
            temp_file_path,
            "values_pedigree",
            "environments",
            "values_metadata",
            "archive_metadata",
            "values_data",
            "values_destinies",
            "persisted_values",
            "aliases",
        )

        result = run_sql_query('SELECT * FROM "aliases";', temp_file_path)

        assert len(result[0]) == 2
        assert result[0][0] in ["result_1", "result_2"]
        assert uuid.UUID(result[0][1])

        assert len(result[1]) == 2
        assert result[1][0] in ["result_1", "result_2"]
        assert uuid.UUID(result[1][1])
