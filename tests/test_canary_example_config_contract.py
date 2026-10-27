"""
Regression test for public canary configuration template contracts.
Verifies config.canary.example.json schema, placeholders, and fail-closed security invariants.
"""
import json
import re
from pathlib import Path
import pytest

from app.canary import (
    load_explicit_config,
    validate_canary_config,
    reject_placeholder_targets,
    is_production_thread,
    is_production_conversation,
    is_production_workspace,
)
from app.models import AppConfig

BASE_DIR = Path(__file__).resolve().parent.parent
CANARY_EXAMPLE_PATH = BASE_DIR / "config.canary.example.json"


def test_canary_example_config_file_exists():
    assert CANARY_EXAMPLE_PATH.is_file(), "config.canary.example.json must exist in repository root"


def test_canary_example_config_schema_and_placeholders():
    data = json.loads(CANARY_EXAMPLE_PATH.read_text(encoding="utf-8"))
    assert data.get("canary_mode") is True
    assert "codex" in data
    assert "chatgpt" in data
    assert "route" in data
    assert "server" in data

    # Verify placeholder IDs are present
    assert "canary" in data["codex"]["thread_id"].lower()
    assert "canary" in data["chatgpt"]["conversation_url"].lower()

    # Verify no personal/production identifiers leaked in template
    raw_text = CANARY_EXAMPLE_PATH.read_text(encoding="utf-8")
    assert "<USER>" in raw_text
    assert "canary_workspace" in raw_text


def test_canary_example_config_fails_closed_when_unmodified():
    """Unmodified template must fail closed with explicit ValueError on placeholder detection."""
    with pytest.raises(ValueError, match="(?i)placeholder"):
        load_explicit_config(CANARY_EXAMPLE_PATH, canary=True)


def test_canary_example_rejects_placeholders_individually():
    with pytest.raises(ValueError, match="Placeholder"):
        reject_placeholder_targets("00000000-canary-0000-0000-000000000001", "https://chatgpt.com/c/real-id")

    with pytest.raises(ValueError, match="Placeholder"):
        reject_placeholder_targets("00000000-0000-0000-0000-000000000000", "https://chatgpt.com/c/00000000-canary-0000")


def test_production_guard_contracts():
    """Verify production guard defaults and dynamic registration contracts."""
    assert is_production_thread("00000000-0000-0000-0000-000000000001")
    assert not is_production_thread("00000000-canary-0000-0000-000000000001")

    assert is_production_conversation("00000000-0000-0000-0000-000000000002")
    assert not is_production_conversation("00000000-canary-0000-0000-000000000002")

    assert is_production_workspace("D:/workspace/production-repo")
    assert not is_production_workspace(BASE_DIR / "canary_workspace")
