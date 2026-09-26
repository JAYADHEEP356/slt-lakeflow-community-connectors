"""Lakeflow connector for CSV files exposed through Knox WebHDFS."""

from __future__ import annotations

import csv
import io
import json
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Iterator
from urllib.parse import quote

import requests
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.labs.community_connector.interface import LakeflowConnect

# The connector publisher flattens this module with framework constants. Keep
# this name specific so it cannot be overwritten by the framework's TABLE_NAME
# request-option key in the generated source.
CLICKSTREAM_TABLE_NAME = "clickstream"
REQUIRED_COLUMNS = (
    "event_time",
    "tower_id",
    "msisdn",
    "url",
    "bytes_used",
    "session_id",
)
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), False),
        StructField("tower_id", StringType(), False),
        StructField("msisdn", StringType(), False),
        StructField("url", StringType(), False),
        StructField("bytes_used", LongType(), False),
        StructField("session_id", StringType(), False),
        StructField("_source_file", StringType(), False),
        StructField("_source_file_size", LongType(), False),
        StructField("_source_modified_time", LongType(), False),
        StructField("_source_row_number", LongType(), False),
        StructField("_source_file_identity", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_rescued_data", StringType(), True),
    ]
)


class WebHdfsError(RuntimeError):
    """Base error returned by the WebHDFS source."""


class WebHdfsAuthenticationError(WebHdfsError):
    """The source rejected the configured credentials."""


class WebHdfsConnectionError(WebHdfsError):
    """The WebHDFS endpoint could not be reached."""


class MalformedCsvError(WebHdfsError):
    """A source file does not conform to the expected CSV contract."""


class UnstableFileError(WebHdfsError):
    """A file changed between discovery and download."""


class ClouderaWebhdfsLakeflowConnect(LakeflowConnect):
    """Read immutable clickstream CSV files incrementally from Knox WebHDFS."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._base_url = options.get("base_url", "").rstrip("/")
        self._username = options.get("username", "")
        self._password = options.get("password", "")
        self._root_path = self._normalise_path(options.get("root_path", ""))
        self._timeout = float(options.get("request_timeout_seconds", "30"))
        self._max_retries = int(options.get("max_retries", "3"))
        self._verify_tls: bool | str = options.get("ca_bundle", True)

        missing = [
            key
            for key, value in (
                ("base_url", self._base_url),
                ("username", self._username),
                ("password", self._password),
                ("root_path", self._root_path),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required connection options: {', '.join(missing)}")

    @staticmethod
    def _normalise_path(path: str) -> str:
        if not path:
            return ""
        return "/" + path.strip("/")

    def _url(self, path: str) -> str:
        encoded_path = quote(self._normalise_path(path), safe="/")
        return f"{self._base_url}{encoded_path}"

    def _request(self, method: str, path: str, *, operation: str) -> requests.Response:
        last_response: requests.Response | None = None
        for attempt in range(self._max_retries):
            try:
                response = requests.request(
                    method,
                    self._url(path),
                    params={"op": operation},
                    auth=(self._username, self._password),
                    timeout=self._timeout,
                    verify=self._verify_tls,
                    allow_redirects=True,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                raise WebHdfsConnectionError(
                    f"Could not reach WebHDFS endpoint '{self._base_url}': {exc}"
                ) from exc

            if response.status_code in (401, 403):
                raise WebHdfsAuthenticationError(
                    f"WebHDFS rejected the configured credentials (HTTP {response.status_code})"
                )
            if response.status_code not in RETRIABLE_STATUS_CODES:
                response.raise_for_status()
                return response

            last_response = response
            if attempt < self._max_retries - 1:
                time.sleep(0.25 * (2**attempt))

        assert last_response is not None
        raise WebHdfsError(
            f"WebHDFS {operation} failed after {self._max_retries} attempts "
            f"(HTTP {last_response.status_code})"
        )

    def _validate_table(self, table_name: str) -> None:
        if table_name != CLICKSTREAM_TABLE_NAME:
            raise ValueError(
                f"Unsupported table '{table_name}'. Supported table: {CLICKSTREAM_TABLE_NAME}"
            )

    def list_tables(self) -> list[str]:
        self._list_files()
        return [CLICKSTREAM_TABLE_NAME]

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return CLICKSTREAM_SCHEMA

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return {
            "primary_keys": [],
            "cursor_field": "_source_file_identity",
            "ingestion_type": "append",
        }

    def _list_files(self) -> list[dict]:
        response = self._request("GET", self._root_path, operation="LISTSTATUS")
        statuses = response.json().get("FileStatuses", {}).get("FileStatus", [])
        files = []
        for status in statuses:
            name = status.get("pathSuffix", "")
            if (
                status.get("type") == "FILE"
                and name.lower().endswith(".csv")
                and not name.startswith((".", "_"))
                and not name.lower().endswith((".tmp.csv", ".partial.csv"))
            ):
                files.append(status)
        return sorted(files, key=lambda item: (int(item["modificationTime"]), item["pathSuffix"]))

    @staticmethod
    def _position(file_status: dict) -> tuple[int, str]:
        return int(file_status["modificationTime"]), str(file_status["pathSuffix"])

    @staticmethod
    def _offset_position(offset: dict | None) -> tuple[int, str]:
        if not offset:
            return -1, ""
        return int(offset.get("modification_time", -1)), str(offset.get("path", ""))

    def _file_path(self, filename: str) -> str:
        return str(PurePosixPath(self._root_path) / filename)

    def _assert_file_stable(self, file_status: dict) -> None:
        response = self._request(
            "GET", self._file_path(file_status["pathSuffix"]), operation="GETFILESTATUS"
        )
        current = response.json()["FileStatus"]
        if int(current["length"]) != int(file_status["length"]) or int(
            current["modificationTime"]
        ) != int(file_status["modificationTime"]):
            raise UnstableFileError(
                f"Source file changed during discovery: {file_status['pathSuffix']}"
            )

    def _parse_file(self, file_status: dict) -> list[dict]:
        self._assert_file_stable(file_status)
        path = self._file_path(file_status["pathSuffix"])
        response = self._request("GET", path, operation="OPEN")

        try:
            reader = csv.DictReader(io.StringIO(response.text, newline=""), strict=True)
            headers = tuple(reader.fieldnames or ())
            missing_headers = [column for column in REQUIRED_COLUMNS if column not in headers]
            if missing_headers:
                raise MalformedCsvError(
                    f"{file_status['pathSuffix']} is missing required columns: "
                    f"{', '.join(missing_headers)}"
                )

            ingested_at = datetime.now(timezone.utc).isoformat()
            identity = (
                f"{file_status['modificationTime']}:{file_status['pathSuffix']}:"
                f"{file_status['length']}"
            )
            records = []
            for row_number, row in enumerate(reader, start=2):
                if None in row:
                    raise MalformedCsvError(
                        f"{file_status['pathSuffix']} row {row_number} has extra unlabelled values"
                    )
                missing_values = [column for column in REQUIRED_COLUMNS if not row.get(column)]
                if missing_values:
                    raise MalformedCsvError(
                        f"{file_status['pathSuffix']} row {row_number} has empty required values: "
                        f"{', '.join(missing_values)}"
                    )
                try:
                    bytes_used = int(row["bytes_used"])
                    datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
                except (TypeError, ValueError) as exc:
                    raise MalformedCsvError(
                        f"{file_status['pathSuffix']} row {row_number} has invalid typed values"
                    ) from exc

                extras = {key: value for key, value in row.items() if key not in REQUIRED_COLUMNS}
                records.append(
                    {
                        "event_time": row["event_time"],
                        "tower_id": row["tower_id"],
                        "msisdn": row["msisdn"],
                        "url": row["url"],
                        "bytes_used": bytes_used,
                        "session_id": row["session_id"],
                        "_source_file": path,
                        "_source_file_size": int(file_status["length"]),
                        "_source_modified_time": int(file_status["modificationTime"]),
                        "_source_row_number": row_number,
                        "_source_file_identity": identity,
                        "_ingested_at": ingested_at,
                        "_rescued_data": json.dumps(extras, sort_keys=True) if extras else None,
                    }
                )
        except csv.Error as exc:
            raise MalformedCsvError(
                f"Could not parse CSV file {file_status['pathSuffix']}: {exc}"
            ) from exc

        self._assert_file_stable(file_status)
        return records

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        max_files = int(table_options.get("max_files_per_batch", "100"))
        if max_files < 1:
            raise ValueError("max_files_per_batch must be at least 1")

        start_position = self._offset_position(start_offset)
        candidates = [
            status for status in self._list_files() if self._position(status) > start_position
        ][:max_files]
        if not candidates:
            return iter([]), start_offset or {}

        records: list[dict] = []
        for status in candidates:
            records.extend(self._parse_file(status))

        last = candidates[-1]
        end_offset = {
            "modification_time": int(last["modificationTime"]),
            "path": str(last["pathSuffix"]),
            "length": int(last["length"]),
        }
        if start_offset and end_offset == start_offset:
            return iter([]), start_offset
        return iter(records), end_offset
