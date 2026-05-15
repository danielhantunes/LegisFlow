"""Pure tests for controlled reconciliation id shapes (no Azure SDK imports)."""

from __future__ import annotations

import re
import uuid


def test_reconciliation_partition_key_contract() -> None:
    assert f"reco_{'proposicoes'.strip().lower()}" == "reco_proposicoes"


def test_proposicoes_recoctl_id_shape() -> None:
    rid = f"proposicoes_recoctl_{uuid.uuid4().hex[:16]}"
    assert rid.startswith("proposicoes_recoctl_")
    assert re.match(r"^proposicoes_recoctl_[a-f0-9]{16}$", rid)
