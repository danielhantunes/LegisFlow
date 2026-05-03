from __future__ import annotations

import json
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


class AdlsRawWriter:
    def __init__(self, account_name: str, filesystem_name: str = "lakehouse") -> None:
        account_url = f"https://{account_name}.dfs.core.windows.net"
        self.client = DataLakeServiceClient(account_url=account_url, credential=DefaultAzureCredential())
        self.fs_client = self.client.get_file_system_client(filesystem_name)

    def write_json(self, path: str, payload: dict[str, Any]) -> str:
        """Write JSON to ADLS. Overwrites if the path already exists (idempotent replay)."""
        content = json.dumps(payload, ensure_ascii=False)
        data = content.encode("utf-8")
        length = len(data)
        file_client = self.fs_client.get_file_client(path)
        try:
            file_client.delete_file()
        except Exception:
            # File may not exist on first write
            pass
        file_client.create_file()
        file_client.append_data(data, offset=0, length=length)
        file_client.flush_data(length)
        return path
