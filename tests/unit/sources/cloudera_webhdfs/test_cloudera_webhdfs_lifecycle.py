"""Live lifecycle tests for the Cloudera WebHDFS connector practice endpoint."""

from __future__ import annotations

import os
import time
import uuid
from urllib.parse import quote

import pytest
import requests

from databricks.labs.community_connector.sources.cloudera_webhdfs.cloudera_webhdfs import (
    ClouderaWebhdfsLakeflowConnect,
    MalformedCsvError,
    WebHdfsAuthenticationError,
    WebHdfsConnectionError,
)

HEADER = "event_time,tower_id,msisdn,url,bytes_used,session_id\n"


def _required_environment() -> dict[str, str]:
    names = ("WEBHDFS_BASE_URL", "WEBHDFS_USERNAME", "WEBHDFS_PASSWORD")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Live WebHDFS variables are not configured: {', '.join(missing)}")
    return {name: os.environ[name] for name in names}


class WebHdfsFixture:
    def __init__(self, base_url: str, username: str, password: str, root_path: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.root_path = "/" + root_path.strip("/")

    def _url(self, path: str) -> str:
        return f"{self.base_url}{quote('/' + path.strip('/'), safe='/')}"

    def request(self, method: str, path: str, operation: str, **params) -> requests.Response:
        response = requests.request(
            method,
            self._url(path),
            params={"op": operation, **params},
            auth=self.auth,
            timeout=30,
            verify=True,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response

    def mkdir(self) -> None:
        result = self.request("PUT", self.root_path, "MKDIRS").json()
        assert result["boolean"] is True

    def create(self, filename: str, content: str) -> None:
        response = requests.put(
            self._url(f"{self.root_path}/{filename}"),
            params={"op": "CREATE", "overwrite": "true"},
            auth=self.auth,
            data=content.encode("utf-8"),
            headers={"Content-Type": "text/csv"},
            timeout=30,
            verify=True,
            allow_redirects=True,
        )
        response.raise_for_status()

    def delete_file(self, filename: str) -> None:
        result = self.request(
            "DELETE", f"{self.root_path}/{filename}", "DELETE", recursive="false"
        ).json()
        assert result["boolean"] is True

    def cleanup(self) -> None:
        try:
            self.request("DELETE", self.root_path, "DELETE", recursive="true")
        except requests.RequestException:
            pass


def test_complete_incremental_file_lifecycle() -> None:
    """Exercise initial, empty, new, late, bad-input, failure, and restart cases."""
    environment = _required_environment()
    parent = os.environ.get("WEBHDFS_TEST_PARENT", "/clickstream/connector-tests")
    root_path = f"{parent.rstrip('/')}/run_{uuid.uuid4().hex}"
    fixture = WebHdfsFixture(
        environment["WEBHDFS_BASE_URL"],
        environment["WEBHDFS_USERNAME"],
        environment["WEBHDFS_PASSWORD"],
        root_path,
    )
    connection_options = {
        "base_url": environment["WEBHDFS_BASE_URL"],
        "username": environment["WEBHDFS_USERNAME"],
        "password": environment["WEBHDFS_PASSWORD"],
        "root_path": root_path,
        "request_timeout_seconds": "10",
    }

    fixture.mkdir()
    try:
        # 1. First ingestion reads all existing files.
        fixture.create(
            "clickstream_20260924_1000.csv",
            HEADER
            + "2026-09-24T10:00:00Z,T001,94770000001,https://example.com/a,100,S001\n"
            + "2026-09-24T10:00:01Z,T002,94770000002,https://example.com/b,200,S002\n",
        )
        fixture.create(
            "clickstream_20260924_1005.csv",
            HEADER + "2026-09-24T10:05:00Z,T003,94770000003,https://example.com/c,300,S003\n",
        )
        connector = ClouderaWebhdfsLakeflowConnect(connection_options)
        records, offset_1 = connector.read_table("clickstream", {}, {})
        first_rows = list(records)
        assert len(first_rows) == 3
        assert {row["session_id"] for row in first_rows} == {"S001", "S002", "S003"}

        # 2. Reusing the checkpoint returns no files and the same checkpoint.
        records, unchanged_offset = connector.read_table("clickstream", offset_1, {})
        assert list(records) == []
        assert unchanged_offset == offset_1

        # 3. A newly published file is the only file returned.
        time.sleep(1.1)
        fixture.create(
            "clickstream_20260924_1010.csv",
            HEADER + "2026-09-24T10:10:00Z,T004,94770000004,https://example.com/d,400,S004\n",
        )
        records, offset_2 = connector.read_table("clickstream", offset_1, {})
        new_rows = list(records)
        assert [row["session_id"] for row in new_rows] == ["S004"]
        assert offset_2 != offset_1

        # 4. An older-named, late-arriving file is found by its new HDFS modification time.
        time.sleep(1.1)
        fixture.create(
            "clickstream_20260924_0955_late.csv",
            HEADER + "2026-09-24T09:55:00Z,T005,94770000005,https://example.com/late,500,S005\n",
        )
        records, offset_3 = connector.read_table("clickstream", offset_2, {})
        late_rows = list(records)
        assert [row["session_id"] for row in late_rows] == ["S005"]

        # 5. A malformed file fails the batch, leaving offset_3 safe to retry.
        time.sleep(1.1)
        malformed_name = "clickstream_20260924_1015_bad.csv"
        fixture.create(
            malformed_name,
            "event_time,tower_id,msisdn,url,bytes_used\n"
            "2026-09-24T10:15:00Z,T006,94770000006,https://example.com/bad,600\n",
        )
        with pytest.raises(MalformedCsvError):
            connector.read_table("clickstream", offset_3, {})

        # 6. Invalid Knox credentials are reported as authentication failure.
        invalid_auth = dict(connection_options)
        invalid_auth["password"] = "definitely-not-the-password"
        with pytest.raises(WebHdfsAuthenticationError):
            ClouderaWebhdfsLakeflowConnect(invalid_auth).read_table("clickstream", {}, {})

        # 7. An unreachable endpoint is reported as a network failure.
        invalid_network = dict(connection_options)
        invalid_network["base_url"] = "https://127.0.0.1:1/webhdfs/v1"
        invalid_network["request_timeout_seconds"] = "1"
        with pytest.raises(WebHdfsConnectionError):
            ClouderaWebhdfsLakeflowConnect(invalid_network).read_table("clickstream", {}, {})

        # 8. A new connector process resumes from offset_3 without duplicates.
        fixture.delete_file(malformed_name)
        restarted = ClouderaWebhdfsLakeflowConnect(connection_options)
        records, restart_offset = restarted.read_table("clickstream", offset_3, {})
        assert list(records) == []
        assert restart_offset == offset_3
    finally:
        fixture.cleanup()
