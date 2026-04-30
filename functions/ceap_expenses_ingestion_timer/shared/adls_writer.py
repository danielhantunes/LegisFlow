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
        content = json.dumps(payload, ensure_ascii=False)
        file_client = self.fs_client.get_file_client(path)
        file_client.create_file()
        file_client.append_data(content, offset=0, length=len(content.encode("utf-8")))
        file_client.flush_data(len(content.encode("utf-8")))
        return path
