"""Tests for the admin reset guard."""

from __future__ import annotations

from shared.admin_guard import GLOBAL_RESET_FLAG_ENV, reset_enabled_for_domain


def test_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv(GLOBAL_RESET_FLAG_ENV, raising=False)
    monkeypatch.delenv("ENABLE_REFERENCE_RESET_FUNCTION", raising=False)
    assert not reset_enabled_for_domain("ENABLE_REFERENCE_RESET_FUNCTION")


def test_enabled_via_global_flag(monkeypatch) -> None:
    monkeypatch.setenv(GLOBAL_RESET_FLAG_ENV, "true")
    monkeypatch.delenv("ENABLE_REFERENCE_RESET_FUNCTION", raising=False)
    assert reset_enabled_for_domain("ENABLE_REFERENCE_RESET_FUNCTION")


def test_enabled_via_domain_flag(monkeypatch) -> None:
    monkeypatch.delenv(GLOBAL_RESET_FLAG_ENV, raising=False)
    monkeypatch.setenv("ENABLE_REFERENCE_RESET_FUNCTION", "true")
    assert reset_enabled_for_domain("ENABLE_REFERENCE_RESET_FUNCTION")


def test_strict_truthy_check(monkeypatch) -> None:
    monkeypatch.setenv(GLOBAL_RESET_FLAG_ENV, "1")
    assert not reset_enabled_for_domain("X")
    monkeypatch.setenv(GLOBAL_RESET_FLAG_ENV, "TRUE")
    assert reset_enabled_for_domain("X")
