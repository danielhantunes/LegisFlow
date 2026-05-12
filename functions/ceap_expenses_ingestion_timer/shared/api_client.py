from __future__ import annotations

from typing import Any

import requests

from .retry import run_with_retry


class CamaraApiClient:
    def __init__(self, base_url: str = "https://dadosabertos.camara.leg.br/api/v2", timeout: int = 45) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, url: str, params: dict[str, Any] | None = None, *, max_attempts: int = 5) -> tuple[dict[str, Any], int]:
        """Returns (json_body, http_status)."""

        def _request() -> tuple[dict[str, Any], int]:
            response = self.session.get(url, params=params, timeout=self.timeout)
            status = int(response.status_code)
            response.raise_for_status()
            return response.json(), status

        return run_with_retry(_request, max_attempts=max_attempts)

    def list_deputies_page(self, page: int = 1, itens: int = 100) -> tuple[dict[str, Any], int]:
        return self._get(f"{self.base_url}/deputados", params={"pagina": page, "itens": itens})

    def list_expenses_page(
        self, deputy_id: int, ano: int, mes: int, page: int = 1, itens: int = 100
    ) -> tuple[dict[str, Any], int]:
        return self._get(
            f"{self.base_url}/deputados/{deputy_id}/despesas",
            params={"ano": ano, "mes": mes, "pagina": page, "itens": itens},
        )

    def list_endpoint_page(
        self,
        path: str,
        *,
        page: int = 1,
        itens: int = 100,
        params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Generic GET for paginated list endpoints (``pagina`` / ``itens``).

        ``path`` may be a relative path (``/partidos``) or fully-formed URL.
        ``params`` is merged on top of pagination params (callers can override
        ``itens`` or add filters such as ``idLegislatura``).
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        merged: dict[str, Any] = {"pagina": page, "itens": itens}
        if params:
            merged.update(params)
        return self._get(url, params=merged)

    def list_votacoes_page(
        self,
        *,
        page: int = 1,
        itens: int = 200,
        date_start: str | None = None,
        date_end: str | None = None,
        ordenar_por: str = "dataHoraRegistro",
        ordem: str = "DESC",
    ) -> tuple[dict[str, Any], int]:
        """GET ``/votacoes`` with optional date window (microbatch / recon)."""
        params: dict[str, Any] = {
            "ordenarPor": ordenar_por,
            "ordem": ordem,
        }
        if date_start:
            params["dataInicio"] = date_start
        if date_end:
            params["dataFim"] = date_end
        return self.list_endpoint_page(
            "/votacoes", page=page, itens=itens, params=params
        )

    def list_votacao_votos_page(
        self,
        votacao_id: str,
        *,
        page: int = 1,
        itens: int = 200,
    ) -> tuple[dict[str, Any], int]:
        """GET ``/votacoes/{id}/votos`` paginated."""
        return self.list_endpoint_page(
            f"/votacoes/{votacao_id}/votos",
            page=page,
            itens=itens,
        )

    def list_proposicoes_page(
        self,
        *,
        page: int = 1,
        itens: int = 100,
        date_start: str | None = None,
        date_end: str | None = None,
        ordenar_por: str = "id",
        ordem: str = "ASC",
    ) -> tuple[dict[str, Any], int]:
        """GET ``/proposicoes`` filtered by tramitação update window.

        ``dataInicio`` / ``dataFim`` filter by **last tramitação update**
        (``YYYY-MM-DD``), which is what we want for microbatch (re-process
        every proposition whose tramitação changed in the window).
        """
        params: dict[str, Any] = {
            "ordenarPor": ordenar_por,
            "ordem": ordem,
        }
        if date_start:
            params["dataInicio"] = date_start
        if date_end:
            params["dataFim"] = date_end
        return self.list_endpoint_page(
            "/proposicoes", page=page, itens=itens, params=params
        )

    def list_proposicao_autores_page(
        self,
        proposicao_id: str,
        *,
        page: int = 1,
        itens: int = 100,
    ) -> tuple[dict[str, Any], int]:
        """GET ``/proposicoes/{id}/autores`` paginated."""
        return self.list_endpoint_page(
            f"/proposicoes/{proposicao_id}/autores",
            page=page,
            itens=itens,
        )

    def list_proposicao_tramitacoes_page(
        self,
        proposicao_id: str,
        *,
        page: int = 1,
        itens: int = 100,
    ) -> tuple[dict[str, Any], int]:
        """GET ``/proposicoes/{id}/tramitacoes`` paginated."""
        return self.list_endpoint_page(
            f"/proposicoes/{proposicao_id}/tramitacoes",
            page=page,
            itens=itens,
        )
