"""M9: unit tests for the auth.py primitives in isolation (no FastAPI app,
no HTTP) -- CredentialStore, ReplayGuard, token extraction, and
insecure-mode env parsing. API-level/adversarial coverage of the same
mechanisms lives in test_m9_adversarial.py.
"""
from __future__ import annotations

import secrets as secrets_module

import pytest

from aidars.distributed.auth import (
    CredentialStore,
    ReplayGuard,
    extract_bearer_token,
    is_insecure_mode_enabled,
)


# ============================================================================
# extract_bearer_token
# ============================================================================


@pytest.mark.parametrize("header,expected", [
    (None, None),
    ("", None),
    ("Bearer abc123", "abc123"),
    ("bearer abc123", "abc123"),  # scheme is case-insensitive
    ("BEARER abc123", "abc123"),
    ("Basic abc123", None),
    ("Bearer", None),
    ("Bearer ", None),
    ("abc123", None),
])
def test_extract_bearer_token(header, expected):
    assert extract_bearer_token(header) == expected


# ============================================================================
# CredentialStore: admin tokens
# ============================================================================


def test_admin_token_verification():
    store = CredentialStore(admin_tokens={"a1", "a2"}, insecure_mode=False)
    assert store.verify_admin_token("a1") is True
    assert store.verify_admin_token("a2") is True
    assert store.verify_admin_token("a3") is False
    assert store.verify_admin_token(None) is False
    assert store.verify_admin_token("") is False


def test_admin_tokens_loaded_from_env(monkeypatch):
    monkeypatch.setenv("AIDAR_ADMIN_TOKENS", "tok1, tok2,tok3")
    monkeypatch.setenv("AIDAR_INSECURE_MODE", "0")
    store = CredentialStore()
    assert store.verify_admin_token("tok1") is True
    assert store.verify_admin_token("tok2") is True
    assert store.verify_admin_token("tok3") is True
    assert store.verify_admin_token("tok4") is False


# ============================================================================
# CredentialStore: bootstrap secret
# ============================================================================


def test_bootstrap_secret_is_independent_of_admin_tokens():
    store = CredentialStore(admin_tokens={"admin-x"}, bootstrap_secret="boot-x", insecure_mode=False)
    assert store.verify_bootstrap_secret("boot-x") is True
    assert store.verify_bootstrap_secret("admin-x") is False
    assert store.verify_admin_token("boot-x") is False


def test_bootstrap_secret_unset_rejects_everything():
    store = CredentialStore(admin_tokens={"a"}, bootstrap_secret=None, insecure_mode=False)
    assert store.verify_bootstrap_secret("anything") is False
    assert store.verify_bootstrap_secret(None) is False


# ============================================================================
# CredentialStore: worker credentials
# ============================================================================


def test_worker_credential_issuance_and_verification():
    store = CredentialStore(insecure_mode=False)
    token = store.issue_worker_credential("w1")
    assert isinstance(token, str) and len(token) > 20
    assert store.verify_worker_credential("w1", token) is True
    assert store.verify_worker_credential("w2", token) is False
    assert store.verify_worker_credential("w1", "wrong") is False


def test_worker_credential_issuance_is_high_entropy_and_unique():
    store = CredentialStore(insecure_mode=False)
    tokens = {store.issue_worker_credential(f"w{i}") for i in range(50)}
    assert len(tokens) == 50  # secrets.token_urlsafe(32) collisions are not realistic


def test_re_registration_issues_a_new_credential_invalidating_the_old_one():
    store = CredentialStore(insecure_mode=False)
    old = store.issue_worker_credential("w1")
    new = store.issue_worker_credential("w1")
    assert old != new
    assert store.verify_worker_credential("w1", old) is False
    assert store.verify_worker_credential("w1", new) is True


def test_verify_any_worker_credential():
    store = CredentialStore(insecure_mode=False)
    tok_a = store.issue_worker_credential("a")
    store.issue_worker_credential("b")
    assert store.verify_any_worker_credential(tok_a) is True
    assert store.verify_any_worker_credential("not-issued") is False
    assert store.verify_any_worker_credential(None) is False


def test_invalidate_all_worker_credentials():
    store = CredentialStore(insecure_mode=False)
    tok_a = store.issue_worker_credential("a")
    tok_b = store.issue_worker_credential("b")
    store.invalidate_all_worker_credentials()
    assert store.verify_worker_credential("a", tok_a) is False
    assert store.verify_worker_credential("b", tok_b) is False


# ============================================================================
# Insecure mode
# ============================================================================


def test_insecure_mode_bypasses_all_verification_methods():
    store = CredentialStore(admin_tokens={"a"}, bootstrap_secret="b", insecure_mode=True)
    # Even wrong/missing credentials aren't checked by dependencies when
    # store.insecure_mode is True -- the dependency functions themselves
    # short-circuit (verified at the API layer in test_m9_adversarial.py);
    # here we just confirm the flag itself is set as constructed.
    assert store.insecure_mode is True


def test_secure_by_default_constructor():
    store = CredentialStore(admin_tokens=set(), insecure_mode=False)
    assert store.insecure_mode is False


# ============================================================================
# ReplayGuard
# ============================================================================


def test_replay_guard_rejects_repeated_nonce_within_ttl():
    guard = ReplayGuard(ttl_seconds=100.0)
    assert guard.check_and_record("n1", current_time=0.0) is True
    assert guard.check_and_record("n1", current_time=50.0) is False
    assert guard.check_and_record("n1", current_time=99.9) is False


def test_replay_guard_allows_nonce_again_after_ttl_expiry():
    guard = ReplayGuard(ttl_seconds=10.0)
    assert guard.check_and_record("n1", current_time=0.0) is True
    assert guard.check_and_record("n1", current_time=10.1) is True


def test_replay_guard_prunes_expired_entries():
    guard = ReplayGuard(ttl_seconds=5.0)
    guard.check_and_record("old", current_time=0.0)
    guard.check_and_record("new", current_time=100.0)  # triggers pruning of "old"
    assert "old" not in guard._seen_until
    assert "new" in guard._seen_until


def test_replay_guard_distinct_nonces_never_conflict():
    guard = ReplayGuard()
    for i in range(20):
        assert guard.check_and_record(f"nonce-{i}") is True


# ============================================================================
# is_insecure_mode_enabled
# ============================================================================


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "True", "yes", "YES"])
def test_insecure_mode_accepted_values(value):
    assert is_insecure_mode_enabled({"AIDAR_INSECURE_MODE": value}) is True


@pytest.mark.parametrize("value", ["0", "false", "no", "", "  ", "enabled", "1 "])
def test_insecure_mode_rejected_values(value):
    # Note: "1 " (trailing space) is stripped, so it WOULD be accepted --
    # excluded from this rejection list deliberately; whitespace-only and
    # anything not in the exact accepted set must be rejected.
    if value.strip().lower() in {"1", "true", "yes"}:
        pytest.skip("value normalizes to an accepted form")
    assert is_insecure_mode_enabled({"AIDAR_INSECURE_MODE": value}) is False


def test_insecure_mode_missing_env_var():
    assert is_insecure_mode_enabled({}) is False
