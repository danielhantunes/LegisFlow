"""Single source of truth for domain/endpoint configuration in LegisFlow.

This catalog describes every ingestion domain (CEAP, reference snapshots,
votacoes, ...) and the endpoints they cover. Dispatcher / worker /
poison-handler / replay / reset functions read from this module so that names
of queues, tables, paths and pipeline_run_id prefixes stay consistent across
the codebase and across infrastructure (Terraform).

Notes
-----
* CEAP entries are descriptive only — the live CEAP code keeps using its own
  modules unchanged (this catalog must not break it).
* ``hash_strategy`` is the value advertised inside ``metadata.json``
  (``hash_strategy`` field). All Raw writers in the new domains apply the
  same envelope (``_audit`` + ``_payload_hash`` + ``_record_uid`` +
  ``_record_hash``).
* Every domain owns its **own queue and poison queue**; Table Storage rows
  are partitioned by domain (sharing the same physical tables).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

DEFAULT_API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
DEFAULT_SOURCE_SYSTEM = "camara_dadosabertos"
DEFAULT_HASH_STRATEGY = "payload_and_record_hash_v1"
DEFAULT_AUDIT_FIELDS: tuple[str, ...] = (
    "_audit",
    "_payload_hash",
    "_record_uid",
    "_record_hash",
)
DEFAULT_STALE_AFTER_MINUTES = 60
DEFAULT_LOCK_TTL_MINUTES = 15
DEFAULT_MAX_TASKS_PER_DISPATCH = 1000

# Validation phase schedules (item 15 of the expansion brief).
SCHEDULE_EVERY_10_MIN = "0 */10 * * * *"
SCHEDULE_EVERY_20_MIN = "0 */20 * * * *"

# Default microbatch lookback (votacoes): how far back the dispatcher scans on
# every tick (overlap with the previous tick provides safety net for late
# upstream updates / clock skew).
DEFAULT_MICROBATCH_LOOKBACK_MINUTES = 60


@dataclass(frozen=True)
class EndpointSpec:
    """One API endpoint within a domain.

    ``path_template`` follows the API definition (``/orgaos/{id}/membros``).
    ``parent_field`` (when present) is the placeholder name in ``path_template``
    that depends on a fanout parent (e.g. ``id``).
    """

    name: str
    path_template: str
    paginated: bool = True
    items_per_page: int = 100
    parent_field: str | None = None
    business_key_fields: tuple[str, ...] = field(default_factory=tuple)
    raw_prefix: str = ""  # under ``raw/camara/``; defaults to ``<endpoint>/api/list``


@dataclass(frozen=True)
class DomainSpec:
    """Declarative configuration for one ingestion domain."""

    name: str
    description: str
    pipeline_run_id_prefixes: tuple[str, ...]
    queue_work: str
    queue_poison: str
    state_partition_key: str  # IngestionState PartitionKey for this domain
    runs_partition_key: str  # IngestionControlApi2026 PartitionKey for runs
    locks_partition_key: str  # IngestionControlApi2026 PartitionKey for dispatcher locks
    lock_row_key: str
    schedule_cron: str
    endpoints: tuple[EndpointSpec, ...]
    hash_strategy: str = DEFAULT_HASH_STRATEGY
    audit_fields: tuple[str, ...] = DEFAULT_AUDIT_FIELDS
    stale_after_minutes: int = DEFAULT_STALE_AFTER_MINUTES
    max_tasks_per_dispatch: int = DEFAULT_MAX_TASKS_PER_DISPATCH
    lock_ttl_minutes: int = DEFAULT_LOCK_TTL_MINUTES
    api_base_url: str = DEFAULT_API_BASE_URL
    source_system: str = DEFAULT_SOURCE_SYSTEM
    reset_feature_flag_env: str = ""

    def endpoint(self, name: str) -> EndpointSpec:
        for ep in self.endpoints:
            if ep.name == name:
                return ep
        raise KeyError(f"Endpoint {name!r} not declared in domain {self.name!r}")

    def is_pipeline_run_id_owned_here(self, pipeline_run_id: str) -> bool:
        return any(
            pipeline_run_id.startswith(prefix)
            for prefix in self.pipeline_run_id_prefixes
        )


# --- CEAP (descritivo; código CEAP existente é a fonte real de verdade) ----
CEAP_DOMAIN = DomainSpec(
    name="ceap",
    description="CEAP / despesas (one task per deputy/year/month).",
    pipeline_run_id_prefixes=("ceap_daily_", "ceap_reconciliation_"),
    queue_work="ceap-api-2026-work",
    queue_poison="ceap-api-2026-work-poison",
    state_partition_key="ceap_2026",
    runs_partition_key="_runs",
    locks_partition_key="_locks",
    lock_row_key="ceap_dispatcher_lock",
    schedule_cron=SCHEDULE_EVERY_20_MIN,
    endpoints=(
        EndpointSpec(
            name="deputado_despesas",
            path_template="/deputados/{id}/despesas",
            paginated=True,
            items_per_page=100,
            parent_field="id",
            business_key_fields=(
                "codDocumento",
                "numDocumento",
                "dataDocumento",
                "tipoDespesa",
                "urlDocumento",
                "parcela",
                "numRessarcimento",
                "cnpjCpfFornecedor",
                "valorDocumento",
            ),
            raw_prefix="ceap/api/despesas",
        ),
    ),
    reset_feature_flag_env="ENABLE_CEAP_RESET_FUNCTION",
)


# --- Reference snapshots (partidos, legislaturas, deputados, frentes, orgaos)
REFERENCE_DOMAIN = DomainSpec(
    name="reference",
    description="Reference snapshots: partidos, legislaturas, deputados, frentes, orgaos.",
    pipeline_run_id_prefixes=("reference_snapshot_",),
    queue_work="reference-snapshot-work",
    queue_poison="reference-snapshot-work-poison",
    state_partition_key="reference_2026",
    runs_partition_key="_runs_reference",
    locks_partition_key="_locks_reference",
    lock_row_key="reference_dispatcher_lock",
    schedule_cron=SCHEDULE_EVERY_20_MIN,
    endpoints=(
        EndpointSpec(
            name="partidos",
            path_template="/partidos",
            business_key_fields=("id",),
            raw_prefix="partidos/api/list",
        ),
        EndpointSpec(
            name="legislaturas",
            path_template="/legislaturas",
            business_key_fields=("id",),
            raw_prefix="legislaturas/api/list",
        ),
        EndpointSpec(
            name="deputados",
            path_template="/deputados",
            business_key_fields=("id",),
            raw_prefix="deputados/api/list",
        ),
        EndpointSpec(
            name="frentes",
            path_template="/frentes",
            business_key_fields=("id",),
            raw_prefix="frentes/api/list",
        ),
        EndpointSpec(
            name="orgaos",
            path_template="/orgaos",
            business_key_fields=("id",),
            raw_prefix="orgaos/api/list",
        ),
    ),
    reset_feature_flag_env="ENABLE_REFERENCE_RESET_FUNCTION",
)


# --- Votações (microbatch + reconciliation com fanout) -----------------------
VOTACOES_DOMAIN = DomainSpec(
    name="votacoes",
    description=(
        "Votações: lista por janela (microbatch) e fanout para "
        "/votacoes/{id}/votos (um worker por votação)."
    ),
    pipeline_run_id_prefixes=(
        "votacoes_microbatch_",
        "votacoes_reconciliation_",
    ),
    queue_work="votacoes-api-work",
    queue_poison="votacoes-api-work-poison",
    state_partition_key="votacoes_2026",
    runs_partition_key="_runs_votacoes",
    locks_partition_key="_locks_votacoes",
    lock_row_key="votacoes_dispatcher_lock",
    schedule_cron=SCHEDULE_EVERY_10_MIN,
    endpoints=(
        EndpointSpec(
            name="votacoes",
            path_template="/votacoes",
            paginated=True,
            items_per_page=200,
            business_key_fields=("id",),
            raw_prefix="votacoes/api/list",
        ),
        EndpointSpec(
            # NOTE: ``/votacoes/{id}/votos`` does NOT echo ``idVotacao`` in
            # each item; the dispatcher/worker pass it via ``parent_id`` which
            # is recorded in ``_audit._parent_id``. The per-record UID below
            # uses ``deputado_.id`` + ``tipoVoto`` and is therefore unique
            # *within* a votação's votes page set. Joins on (votacao_id,
            # deputado_id) should always carry the parent context from
            # ``_audit._parent_id`` (or the partition path).
            name="votacao_votos",
            path_template="/votacoes/{id}/votos",
            paginated=True,
            items_per_page=200,
            parent_field="id",
            business_key_fields=("deputado_.id", "tipoVoto"),
            raw_prefix="votacoes/api/votos",
        ),
    ),
    reset_feature_flag_env="ENABLE_VOTACOES_RESET_FUNCTION",
)


_PIPELINE_RUN_ID_RE = re.compile(r"^[a-z0-9_]{1,80}_\d{8}(?:\d{4})?$")


def is_well_formed_pipeline_run_id(pipeline_run_id: str) -> bool:
    """Generic shape check (e.g. ``reference_snapshot_20260511``).

    Each domain's reset/replay code also enforces a domain-specific prefix on
    top of this generic shape check.
    """
    return bool(_PIPELINE_RUN_ID_RE.match((pipeline_run_id or "").strip()))


_DOMAINS: dict[str, DomainSpec] = {
    CEAP_DOMAIN.name: CEAP_DOMAIN,
    REFERENCE_DOMAIN.name: REFERENCE_DOMAIN,
    VOTACOES_DOMAIN.name: VOTACOES_DOMAIN,
}


def get_domain(name: str) -> DomainSpec:
    try:
        return _DOMAINS[name]
    except KeyError as exc:
        known = ", ".join(sorted(_DOMAINS))
        raise KeyError(
            f"Unknown domain {name!r}; known domains: {known}"
        ) from exc


def list_domains() -> tuple[DomainSpec, ...]:
    return tuple(_DOMAINS[name] for name in sorted(_DOMAINS))


def reference_run_id_for_date(reference_date: str) -> str:
    """Deterministic pipeline_run_id for a daily reference snapshot.

    ``reference_date`` is ``YYYY-MM-DD`` (snapshot day). Always returns
    ``reference_snapshot_YYYYMMDD`` so the same call on the same day is
    idempotent and matches the prefix declared in ``REFERENCE_DOMAIN``.
    """
    compact = (reference_date or "").replace("-", "")
    if not compact:
        raise ValueError("reference_date must be YYYY-MM-DD")
    return f"reference_snapshot_{compact}"


def votacoes_microbatch_run_id(reference_minute_utc: str) -> str:
    """Deterministic pipeline_run_id for a votacoes microbatch tick.

    ``reference_minute_utc`` is ``YYYY-MM-DDTHH:MM`` (anchor minute, UTC,
    rounded down to the dispatch granularity). Returns
    ``votacoes_microbatch_YYYYMMDDHHMM`` so two ticks on the same minute
    produce the same id (idempotent retries).
    """
    raw = (reference_minute_utc or "").strip()
    if not raw:
        raise ValueError("reference_minute_utc must be YYYY-MM-DDTHH:MM")
    compact = raw.replace("-", "").replace("T", "").replace(":", "")
    if len(compact) < 12:
        raise ValueError("reference_minute_utc must include minute")
    return f"votacoes_microbatch_{compact[:12]}"


def votacoes_reconciliation_run_id(reference_date: str) -> str:
    compact = (reference_date or "").replace("-", "")
    if not compact:
        raise ValueError("reference_date must be YYYY-MM-DD")
    return f"votacoes_reconciliation_{compact}"
