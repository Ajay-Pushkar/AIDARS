"""M9: Coordinator authentication foundation.

Two operational identity classes, plus one provisioning-only secret --
locked by prior architecture review, not open for silent expansion here:

  1. Admin/client token   -- operator-configured (env), static, used for
                              all control-plane operations (job/workload
                              submission and inspection, cluster stats,
                              worker registry inspection).
  2. Per-worker credential -- coordinator-issued at successful
                              registration, ephemeral (in-memory only,
                              never persisted), invalidated on restart.
                              Required on every subsequent call bound to
                              that specific worker_id.
  3. Bootstrap/join secret -- operator-configured (env), static,
                              authorizes ONLY POST /workers/register.
                              Provisioning material, not a third
                              operational identity: it never authorizes
                              anything else and is never returned by any
                              API response.

No RBAC, no tenants, no roles, no permissions, no credential rotation.
Explicitly out of scope per the locked M9 architecture.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import threading
import time
from typing import Dict, Optional, Set

from fastapi import Header, HTTPException, status

logger = logging.getLogger(__name__)

INSECURE_MODE_ENV_VAR = "AIDAR_INSECURE_MODE"
ADMIN_TOKENS_ENV_VAR = "AIDAR_ADMIN_TOKENS"
BOOTSTRAP_SECRET_ENV_VAR = "AIDAR_WORKER_BOOTSTRAP_SECRET"

# Only these exact (case-insensitive) values enable insecure mode. Anything
# else -- unset, empty, "0", "false", garbage -- keeps the secure default.
# Missing/invalid credentials must never be silently treated as an implicit
# opt-in; only this explicit env signal can disable authentication.
_ACCEPTED_INSECURE_VALUES = {"1", "true", "yes"}


def is_insecure_mode_enabled(env: Optional[Dict[str, str]] = None) -> bool:
    source = env if env is not None else os.environ
    value = source.get(INSECURE_MODE_ENV_VAR, "")
    return value.strip().lower() in _ACCEPTED_INSECURE_VALUES


def _hash_token(token: str) -> str:
    """SHA-256 of a high-entropy, machine-generated bearer token.

    Not a password KDF (bcrypt/scrypt/argon2) on purpose: those exist to
    slow down brute-forcing a small, human-chosen keyspace. A
    secrets.token_urlsafe(32) token has 256 bits of entropy -- brute
    force is infeasible regardless of hash speed, so a slow KDF adds
    cost and complexity (salts, work factors, a new dependency) for no
    realized benefit here. A single fast cryptographic hash, compared
    in constant time, is the correct and standard choice for this
    threat model (the same approach used for e.g. GitHub personal
    access tokens and most cloud API keys).
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    """Parse 'Authorization: Bearer <token>'. Returns None for anything
    else (missing header, wrong scheme, empty token) -- callers treat
    None as "no credential presented", never as a distinct error path
    that could leak information about why it failed."""
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


class CredentialStore:
    """Holds all M9 credential state for one CoordinatorService instance.

    Admin tokens and the bootstrap secret are loaded once from the
    environment (or passed explicitly, e.g. by tests) and never change.
    Worker credentials are minted at registration time and live only in
    memory -- a coordinator restart clears them, forcing every worker to
    re-register (which composes with, not weakens, the existing
    OFFLINE-until-fresh-liveness-proof recovery model).
    """

    def __init__(
        self,
        admin_tokens: Optional[Set[str]] = None,
        bootstrap_secret: Optional[str] = None,
        insecure_mode: Optional[bool] = None,
    ) -> None:
        self._admin_token_hashes: Set[str] = {
            _hash_token(t) for t in (admin_tokens if admin_tokens is not None else self._load_admin_tokens_from_env())
        }
        resolved_bootstrap = bootstrap_secret if bootstrap_secret is not None else self._load_bootstrap_secret_from_env()
        self._bootstrap_secret_hash: Optional[str] = _hash_token(resolved_bootstrap) if resolved_bootstrap else None

        self._worker_credential_hashes: Dict[str, str] = {}
        self._lock = threading.RLock()

        self.insecure_mode = is_insecure_mode_enabled() if insecure_mode is None else insecure_mode
        if self.insecure_mode:
            logger.warning(
                "AIDAR_INSECURE_MODE is ENABLED on CredentialStore -- all authentication "
                "checks are bypassed. This must NEVER be used in production."
            )

    @staticmethod
    def _load_admin_tokens_from_env() -> Set[str]:
        raw = os.environ.get(ADMIN_TOKENS_ENV_VAR, "")
        return {t.strip() for t in raw.split(",") if t.strip()}

    @staticmethod
    def _load_bootstrap_secret_from_env() -> Optional[str]:
        raw = os.environ.get(BOOTSTRAP_SECRET_ENV_VAR, "")
        return raw.strip() or None

    # ------------------------------------------------------------------ #
    # Verification -- all constant-time on hashes, never on plaintext.
    # ------------------------------------------------------------------ #

    def verify_admin_token(self, token: Optional[str]) -> bool:
        if not token:
            return False
        candidate = _hash_token(token)
        return any(secrets.compare_digest(candidate, h) for h in self._admin_token_hashes)

    def verify_bootstrap_secret(self, token: Optional[str]) -> bool:
        if not token or self._bootstrap_secret_hash is None:
            return False
        return secrets.compare_digest(_hash_token(token), self._bootstrap_secret_hash)

    def verify_worker_credential(self, worker_id: str, token: Optional[str]) -> bool:
        if not token:
            return False
        with self._lock:
            stored = self._worker_credential_hashes.get(worker_id)
        if stored is None:
            return False
        return secrets.compare_digest(_hash_token(token), stored)

    def verify_any_worker_credential(self, token: Optional[str]) -> bool:
        """True if `token` matches ANY currently-registered worker's
        credential -- used only for endpoints open to "any authenticated
        worker" (e.g. /assets/locate) where the caller's specific
        worker_id isn't part of the URL to check against directly."""
        if not token:
            return False
        candidate = _hash_token(token)
        with self._lock:
            hashes = list(self._worker_credential_hashes.values())
        return any(secrets.compare_digest(candidate, h) for h in hashes)

    # ------------------------------------------------------------------ #
    # Issuance / lifecycle
    # ------------------------------------------------------------------ #

    def issue_worker_credential(self, worker_id: str) -> str:
        """Mint a fresh, high-entropy credential for worker_id, replacing
        any previous one (e.g. on re-registration after a crash). Returns
        the plaintext exactly once -- the caller (the /workers/register
        handler) must return it to the worker and never log or persist
        it; only its hash is retained here."""
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._worker_credential_hashes[worker_id] = _hash_token(token)
        return token

    def invalidate_all_worker_credentials(self) -> None:
        """Called conceptually "on restart" -- in practice this just
        means a fresh CredentialStore is constructed with an empty map,
        since nothing persists worker credentials. Exposed explicitly so
        tests can simulate the restart boundary without constructing a
        whole new CoordinatorService."""
        with self._lock:
            self._worker_credential_hashes.clear()


class ReplayGuard:
    """Minimal, bounded, in-memory replay defense.

    Not a distributed nonce service -- a single coordinator process
    tracking nonces it has personally seen, pruned by TTL so memory
    stays bounded regardless of traffic volume. A caller opts in by
    sending an 'X-Request-Nonce' header; a second request presenting a
    nonce already seen within the TTL window is rejected as a replay.
    Requests that omit the header get no nonce-specific replay check
    (ordinary bearer-token authentication still applies) -- this keeps
    the mechanism additive rather than a breaking change to every
    existing caller that doesn't yet send nonces.
    """

    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._ttl = ttl_seconds
        self._seen_until: Dict[str, float] = {}
        self._lock = threading.RLock()

    def check_and_record(self, nonce: str, current_time: Optional[float] = None) -> bool:
        """Returns True if `nonce` is fresh (and records it), False if it
        was already seen within the TTL window (a replay)."""
        now = current_time if current_time is not None else time.time()
        with self._lock:
            expired = [n for n, expiry in self._seen_until.items() if expiry <= now]
            for n in expired:
                del self._seen_until[n]

            if nonce in self._seen_until:
                return False
            self._seen_until[nonce] = now + self._ttl
            return True


# ============================================================================
# FastAPI dependency factories
#
# These are constructed once per CoordinatorService (closing over its
# CredentialStore/ReplayGuard) inside coordinator.py -- kept here as plain
# functions parameterized by the store, rather than as coordinator.py
# closures, so the verification logic itself has one home and is unit
# testable without spinning up a FastAPI app.
# ============================================================================


def make_require_admin(store: CredentialStore):
    def require_admin(authorization: Optional[str] = Header(None)) -> None:
        if store.insecure_mode:
            return
        token = extract_bearer_token(authorization)
        if not store.verify_admin_token(token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing admin credential")
    return require_admin


def make_require_bootstrap(store: CredentialStore):
    def require_bootstrap(authorization: Optional[str] = Header(None)) -> None:
        if store.insecure_mode:
            return
        token = extract_bearer_token(authorization)
        if not store.verify_bootstrap_secret(token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing bootstrap credential")
    return require_bootstrap


def make_require_worker(store: CredentialStore):
    def require_worker(worker_id: str, authorization: Optional[str] = Header(None)) -> None:
        if store.insecure_mode:
            return
        token = extract_bearer_token(authorization)
        if not store.verify_worker_credential(worker_id, token):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing worker credential")
    return require_worker


def make_require_worker_or_admin(store: CredentialStore):
    def require_worker_or_admin(worker_id: str, authorization: Optional[str] = Header(None)) -> None:
        if store.insecure_mode:
            return
        token = extract_bearer_token(authorization)
        if store.verify_admin_token(token) or store.verify_worker_credential(worker_id, token):
            return
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing credential")
    return require_worker_or_admin


def make_require_admin_or_any_worker(store: CredentialStore):
    def require_admin_or_any_worker(authorization: Optional[str] = Header(None)) -> None:
        if store.insecure_mode:
            return
        token = extract_bearer_token(authorization)
        if store.verify_admin_token(token) or store.verify_any_worker_credential(token):
            return
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing credential")
    return require_admin_or_any_worker


def make_check_replay(store: CredentialStore, guard: ReplayGuard):
    def check_replay(x_request_nonce: Optional[str] = Header(None, alias="X-Request-Nonce")) -> None:
        if store.insecure_mode or x_request_nonce is None:
            return
        if not guard.check_and_record(x_request_nonce):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Replayed request (nonce already used)")
    return check_replay
