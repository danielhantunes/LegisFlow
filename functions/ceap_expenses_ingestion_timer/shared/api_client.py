from __future__ import annotations

from typing import Any

import requests

from .retry import run_with_retry


class CamaraApiClient:
    def __init__(self, base_url: str = "https://dadosabertos.camara.leg.br/api/v2", timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        def _request() -> dict[str, Any]:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        return run_with_retry(_request, max_attempts=3)

    def list_deputies_page(self, page: int = 1, itens: int = 100) -> dict[str, Any]:
        return self._get(f"{self.base_url}/deputados", params={"pagina": page, "itens": itens})

    def list_expenses_page(self, deputy_id: int, ano: int, mes: int, page: int = 1, itens: int = 100) -> dict[str, Any]:
        return self._get(
            f"{self.base_url}/deputados/{deputy_id}/despesas",
            params={"ano": ano, "mes": mes, "pagina": page, "itens": itens},
        )
