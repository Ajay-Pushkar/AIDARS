"""M8.8/M8.9: verified-output Artifact model and lifecycle/GC-eligibility tracking.

An Artifact represents a verified WORKLOAD OUTPUT. It is conceptually
distinct from M4's AssetRecord (adapters/blender/packaging/models.py),
which represents an INPUT-side scene dependency resolved before
dispatch -- the two are never merged.

Content identity is always the same SHA-256 the existing CAS layer
already computes (LocalCASAdapter.store_bytes / ExecutionManager's
output ingestion) -- this module introduces no second hashing scheme.
artifact_id is a separate, derived key: f"{producer_workload_id}:{content_hash}",
so at-least-once re-execution that reproduces byte-identical output from
a different workload_id is recorded as its own provenance row rather
than silently colliding with (or overwriting) the first one -- the CAS
layer already deduplicates the underlying bytes; this module tracks who
produced them, not a second copy of the bytes.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set

if TYPE_CHECKING:
    from aidars.distributed.state_store import CoordinatorStateStore


class ArtifactVerificationState(str, Enum):
    VERIFIED = "verified"  # the only state this milestone ever assigns -- see module docstring


class ArtifactLifecycleState(str, Enum):
    """GC-eligibility lifecycle. Transitions are metadata-only: nothing in
    this module ever deletes CAS bytes. Physical reclamation (if any) is
    a future, external, explicitly-invoked operation -- see
    ArtifactRegistry.mark_deleted()'s docstring."""

    CREATED = "created"
    AVAILABLE = "available"
    GC_ELIGIBLE = "gc_eligible"
    DELETED = "deleted"


@dataclass
class Artifact:
    """A single (content_hash, producer_workload_id) provenance record."""

    artifact_id: str
    content_hash: str
    producer_workload_id: str
    producer_job_id: Optional[str]
    created_at: float
    verification_state: ArtifactVerificationState
    lifecycle_state: ArtifactLifecycleState
    storage_location: str
    size_bytes: Optional[int] = None


def make_artifact_id(producer_workload_id: str, content_hash: str) -> str:
    return f"{producer_workload_id}:{content_hash}"


class ArtifactRegistry:
    """Thread-safe in-memory registry of output Artifacts.

    Creation is driven by WorkloadOrchestrator._process_workload() at the
    exact point it already calls WorkloadRegistry.set_result() for a
    COMPLETED result (Phase 5.0's boundary) -- this is an additional
    consumer of that existing hook, not a new dispatcher or a new
    result-persistence mechanism.
    """

    def __init__(self, state_store: Optional["CoordinatorStateStore"] = None) -> None:
        self._lock = threading.RLock()
        self._artifacts: Dict[str, Artifact] = {}  # artifact_id -> Artifact
        self._by_hash: Dict[str, Set[str]] = {}    # content_hash -> {artifact_id, ...}
        self._state_store = state_store

    def _persist_artifact(self, snapshot: Optional[Artifact]) -> None:
        if self._state_store is None or snapshot is None:
            return
        self._state_store.save_artifact(snapshot)

    def record_artifacts(
        self,
        producer_workload_id: str,
        producer_job_id: Optional[str],
        content_hashes: Iterable[str],
        sizes: Optional[Dict[str, int]] = None,
        storage_location: str = "cas",
    ) -> List[Artifact]:
        """Record one Artifact per content_hash for a COMPLETED workload's
        output_asset_hashes. Every hash reaching this point already passed
        M8.6 output verification and was already committed to CAS by
        ExecutionManager, so verification_state is always VERIFIED here --
        this module doesn't re-verify, it records the verification that
        already happened."""
        sizes = sizes or {}
        created: List[Artifact] = []
        with self._lock:
            for content_hash in content_hashes:
                artifact_id = make_artifact_id(producer_workload_id, content_hash)
                if artifact_id in self._artifacts:
                    continue  # idempotent: re-recording the same (workload, hash) pair is a no-op
                artifact = Artifact(
                    artifact_id=artifact_id,
                    content_hash=content_hash,
                    producer_workload_id=producer_workload_id,
                    producer_job_id=producer_job_id,
                    created_at=time.time(),
                    verification_state=ArtifactVerificationState.VERIFIED,
                    lifecycle_state=ArtifactLifecycleState.AVAILABLE,
                    storage_location=storage_location,
                    size_bytes=sizes.get(content_hash),
                )
                self._artifacts[artifact_id] = artifact
                self._by_hash.setdefault(content_hash, set()).add(artifact_id)
                created.append(artifact)
        for artifact in created:
            self._persist_artifact(artifact)
        return created

    def get_artifact(self, artifact_id: str) -> Optional[Artifact]:
        with self._lock:
            return self._artifacts.get(artifact_id)

    def list_artifacts(self) -> List[Artifact]:
        with self._lock:
            return list(self._artifacts.values())

    def get_artifacts_for_hash(self, content_hash: str) -> List[Artifact]:
        with self._lock:
            ids = self._by_hash.get(content_hash, set())
            return [self._artifacts[i] for i in ids if i in self._artifacts]

    def restore_artifact(self, artifact: Artifact) -> None:
        """Insert a fully-formed Artifact directly (loaded from
        CoordinatorStateStore.load_artifacts()). Does not persist."""
        with self._lock:
            self._artifacts[artifact.artifact_id] = artifact
            self._by_hash.setdefault(artifact.content_hash, set()).add(artifact.artifact_id)

    # ------------------------------------------------------------------ #
    # Lifecycle / GC (M8.9)
    # ------------------------------------------------------------------ #

    def compute_gc_eligible(self, protected_job_ids: Set[str]) -> List[str]:
        """Return artifact_ids that are safe to mark GC_ELIGIBLE.

        An artifact is "referenced" (protected) if it has no job context
        (producer_job_id is None -- conservatively always protected) or its
        producer_job_id is in `protected_job_ids` (the caller's set of
        still-non-terminal jobs, e.g. from JobRegistry.get_aggregate()
        being non-terminal). Everything else -- belonging to a job whose
        aggregate has reached a terminal state -- is unreferenced by this
        milestone's definition and becomes GC-eligible.

        This is a pure, on-demand computation: nothing calls this
        automatically (no background sweep), and nothing here deletes
        anything -- see mark_gc_eligible()/mark_deleted().
        """
        with self._lock:
            eligible = []
            for artifact in self._artifacts.values():
                if artifact.lifecycle_state != ArtifactLifecycleState.AVAILABLE:
                    continue
                if artifact.producer_job_id is None:
                    continue
                if artifact.producer_job_id in protected_job_ids:
                    continue
                eligible.append(artifact.artifact_id)
            return eligible

    def mark_gc_eligible(self, artifact_ids: Iterable[str]) -> int:
        """Transition AVAILABLE artifacts to GC_ELIGIBLE. Metadata-only --
        does not touch CAS bytes. Returns the count actually transitioned."""
        count = 0
        snapshots: List[Artifact] = []
        with self._lock:
            for artifact_id in artifact_ids:
                artifact = self._artifacts.get(artifact_id)
                if artifact is None or artifact.lifecycle_state != ArtifactLifecycleState.AVAILABLE:
                    continue
                artifact.lifecycle_state = ArtifactLifecycleState.GC_ELIGIBLE
                count += 1
                snapshots.append(artifact)
        for snapshot in snapshots:
            self._persist_artifact(snapshot)
        return count

    def mark_deleted(self, artifact_id: str) -> bool:
        """Transition a GC_ELIGIBLE artifact to DELETED.

        This ONLY updates the lifecycle_state metadata row -- it never
        calls into CAS to remove the underlying bytes (LocalCASAdapter
        does have a delete_asset(), but invoking physical deletion safely
        requires cross-referencing every OTHER artifact that might still
        point at the same content_hash, plus worker-vs-coordinator CAS
        locality, which is explicitly out of scope for this milestone --
        the PRD asks for lifecycle/GC-eligibility semantics, not an actual
        reclamation sweep. Nothing in this codebase calls mark_deleted()
        automatically; it exists as an explicit, caller-invoked API for a
        future cleanup process to use once real deletion is implemented.
        """
        snapshot: Optional[Artifact] = None
        with self._lock:
            artifact = self._artifacts.get(artifact_id)
            if artifact is None or artifact.lifecycle_state != ArtifactLifecycleState.GC_ELIGIBLE:
                return False
            artifact.lifecycle_state = ArtifactLifecycleState.DELETED
            snapshot = artifact
        self._persist_artifact(snapshot)
        return True
