from __future__ import annotations

import json
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
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

    def write_text(self, path: str, content: str) -> str:
        """Write a text/empty file (used for completion markers like _SUCCESS)."""
        data = content.encode("utf-8")
        length = len(data)
        file_client = self.fs_client.get_file_client(path)
        try:
            file_client.delete_file()
        except Exception:
            pass
        file_client.create_file()
        if length > 0:
            file_client.append_data(data, offset=0, length=length)
            file_client.flush_data(length)
        else:
            file_client.flush_data(0)
        return path

    def path_exists(self, path: str) -> bool:
        file_client = self.fs_client.get_file_client(path)
        try:
            file_client.get_file_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception:
            return False

    def read_json(self, path: str) -> dict[str, Any] | None:
        file_client = self.fs_client.get_file_client(path)
        try:
            downloader = file_client.download_file()
            data = downloader.readall()
            return json.loads(data.decode("utf-8"))
        except ResourceNotFoundError:
            return None
        except Exception:
            return None

    def list_subdirectories(self, prefix: str) -> list[str]:
        """Lists immediate subdirectories under ``prefix`` (full paths from filesystem root)."""
        try:
            results: list[str] = []
            for path in self.fs_client.get_paths(path=prefix, recursive=False):
                if getattr(path, "is_directory", False):
                    results.append(path.name)
            return results
        except ResourceNotFoundError:
            return []
        except Exception:
            return []
