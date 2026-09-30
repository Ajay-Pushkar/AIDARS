"""M10.7: checkpoint capability model and checkpoint metadata validation.

Checkpointing is ONLY exposed through an explicit, honest capability flag
(`RuntimeAdapter.supports_checkpointing`, see runtime.py) -- a runtime
that cannot safely checkpoint must never pretend it can. This module adds
no second checkpoint storage system: checkpoint bytes are ordinary CAS
content (already stored via LocalCASAdapter.store_bytes by
ExecutionManager's normal output-ingestion path), and "does this
checkpoint hash exist anywhere in the cluster" reuses the existing
WorkerRegistry inverted hash index (get_workers_for_hash /
locate_hashes) rather than a new CAS query mechanism. What this module
adds is purely metadata validation on top of that existing identity.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Tuple

if TYPE_CHECKING:
    from aidars.distributed.attempt import AttemptRecord

# Bumped only if the on-disk/CAS representation of a checkpoint's payload
# ever changes shape in a way that makes an older checkpoint unsafe to
# resume from. A checkpoint whose format_version doesn't match the
# runtime's current expectation is treated as incompatible, never
# force-loaded.
CURRENT_CHECKPOINT_FORMAT_VERSION = 1


def validate_checkpoint(
    attempt: "AttemptRecord",
    cas_has_hash_fn: Callable[[str], bool],
) -> Tuple[bool, Optional[str]]:
    """Validate that an attempt's recorded checkpoint is restorable.

    Checks, in order:
      1. The attempt actually recorded a checkpoint (hash present).
      2. The checkpoint's format version is one this coordinator/runtime
         generation understands.
      3. The checkpoint's content hash is actually resolvable somewhere
         in the cluster (via the caller-supplied lookup, which should be
         backed by WorkerRegistry.get_workers_for_hash/locate_hashes or
         an equivalent local CAS check -- never a second identity
         scheme).

    Ownership (belongs to this workload_id/attempt_id) is implicit: the
    checkpoint fields live directly on the AttemptRecord that produced
    them, so there is no separate lookup that could return a checkpoint
    belonging to a different workload or attempt.

    Returns (is_valid, reason_if_invalid).
    """
    if attempt.checkpoint_hash is None:
        return False, "attempt has no recorded checkpoint"

    if attempt.checkpoint_format_version != CURRENT_CHECKPOINT_FORMAT_VERSION:
        return False, (
            f"checkpoint format version {attempt.checkpoint_format_version!r} "
            f"is incompatible with current version {CURRENT_CHECKPOINT_FORMAT_VERSION}"
        )

    if not cas_has_hash_fn(attempt.checkpoint_hash):
        return False, f"checkpoint content hash {attempt.checkpoint_hash!r} not found in cluster CAS"

    return True, None
