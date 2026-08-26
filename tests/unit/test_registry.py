"""Unit tests for WorkerRegistry, inverted hash index, health tracking, and eviction."""
from __future__ import annotations

import threading
import time
from typing import List

import pytest

from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerStatus,
)
from aidars.distributed.registry import (
    WorkerHealthStatus,
    WorkerRegistry,
)

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_CORRUPT = "f" * 64


def make_worker_info(
    worker_id: str,
    ip: str = "192.168.1.10",
    port: int = 8000,
    inventory: set | None = None,
    capacity: int = 1000000,
    used: int = 100000,
) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        port=port,
        capacity_bytes=capacity,
        used_bytes=used,
        inventory_hashes=inventory or set(),
    )


# ============================================================================
# Registration & Lifecycle Tests
# ============================================================================


def test_register_and_get_worker():
    registry = WorkerRegistry()
    w_info = make_worker_info("worker-01", inventory={HASH_A, HASH_B})

    registered = registry.register_worker(w_info)
    assert registered.worker_id == "worker-01"
    assert registry.has_worker("worker-01") is True
    assert registry.get_worker_count() == 1

    retrieved = registry.get_worker("worker-01")
    assert retrieved is not None
    assert retrieved.worker_id == "worker-01"
    assert HASH_A in retrieved.inventory_hashes

    # Test defensive copying: mutating retrieved object must not affect internal state
    retrieved.worker_id = "mutated-id"
    retrieved.inventory_hashes.clear()

    internal = registry.get_worker("worker-01")
    assert internal is not None
    assert internal.worker_id == "worker-01"
    assert len(internal.inventory_hashes) == 2


def test_register_invalid_worker_id():
    registry = WorkerRegistry()
    with pytest.raises(ValueError):
        w_info = WorkerInfo(
            worker_id="invalid id with spaces!",
            endpoint_url="http://192.168.1.1:8000",
            ip_address="192.168.1.1",
            port=8000,
        )
        registry.register_worker(w_info)


def test_register_update_replaces_old_inventory():
    registry = WorkerRegistry()
    w_info1 = make_worker_info("worker-01", inventory={HASH_A, HASH_B})
    registry.register_worker(w_info1)

    assert registry.get_workers_for_hash(HASH_A) == {"worker-01"}
    assert registry.get_workers_for_hash(HASH_B) == {"worker-01"}
    assert registry.get_hash_count() == 2

    # Re-register with only HASH_C
    w_info2 = make_worker_info("worker-01", inventory={HASH_C})
    registry.register_worker(w_info2)

    assert registry.get_workers_for_hash(HASH_A) == set()
    assert registry.get_workers_for_hash(HASH_B) == set()
    assert registry.get_workers_for_hash(HASH_C) == {"worker-01"}
    assert registry.get_hash_count() == 1


def test_unregister_worker_purges_index():
    registry = WorkerRegistry()
    w1 = make_worker_info("worker-01", inventory={HASH_A, HASH_B})
    w2 = make_worker_info("worker-02", inventory={HASH_B, HASH_C})

    registry.register_worker(w1)
    registry.register_worker(w2)

    assert registry.get_workers_for_hash(HASH_B) == {"worker-01", "worker-02"}

    # Unregister worker-01
    removed = registry.unregister_worker("worker-01")
    assert removed is not None
    assert removed.worker_id == "worker-01"

    # worker-01 is removed from HASH_A (which is now completely purged) and HASH_B
    assert registry.get_workers_for_hash(HASH_A) == set()
    assert registry.get_workers_for_hash(HASH_B) == {"worker-02"}
    assert registry.get_workers_for_hash(HASH_C) == {"worker-02"}
    assert registry.get_worker_count() == 1


def test_unregister_nonexistent_worker():
    registry = WorkerRegistry()
    assert registry.unregister_worker("does-not-exist") is None


def test_list_workers_filtering():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-healthy")
    w2 = make_worker_info("w-degraded")
    w3 = make_worker_info("w-suspect")

    registry.register_worker(w1)
    registry.register_worker(w2)
    registry.register_worker(w3)

    # Degrade w2 and make w3 suspect
    registry.record_failure("w-degraded", penalty=4.0)  # > 3.0
    registry.record_failure("w-suspect", penalty=12.0)  # > 10.0

    active = registry.list_workers(active_only=True)
    all_workers = registry.list_workers(active_only=False)

    active_ids = {w.worker_id for w in active}
    all_ids = {w.worker_id for w in all_workers}

    assert "w-healthy" in active_ids
    assert "w-degraded" in active_ids
    assert "w-suspect" not in active_ids
    assert len(all_ids) == 3


# ============================================================================
# Inverted Hash Index Tests
# ============================================================================


def test_inverted_hash_index_locate():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-1", inventory={HASH_A, HASH_B})
    w2 = make_worker_info("w-2", inventory={HASH_B, HASH_C})
    registry.register_worker(w1)
    registry.register_worker(w2)

    located = registry.locate_hashes([HASH_A, HASH_B, HASH_C, "f" * 64])
    assert located[HASH_A] == {"w-1"}
    assert located[HASH_B] == {"w-1", "w-2"}
    assert located[HASH_C] == {"w-2"}
    assert ("f" * 64) not in located


def test_add_and_remove_worker_hashes():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-1", inventory={HASH_A})
    registry.register_worker(w1)

    # Add HASH_B
    added = registry.add_worker_hashes("w-1", [HASH_B])
    assert added == 1
    assert registry.get_workers_for_hash(HASH_B) == {"w-1"}

    # Remove HASH_A
    removed = registry.remove_worker_hashes("w-1", [HASH_A])
    assert removed == 1
    assert registry.get_workers_for_hash(HASH_A) == set()
    assert registry.get_all_indexed_hashes() == {HASH_B}


def test_sync_worker_inventory():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-1", inventory={HASH_A, HASH_B})
    registry.register_worker(w1)

    # Target: HASH_B and HASH_C (HASH_A removed, HASH_C added)
    added, removed = registry.sync_worker_inventory("w-1", {HASH_B, HASH_C})
    assert added == 1
    assert removed == 1

    assert registry.get_workers_for_hash(HASH_A) == set()
    assert registry.get_workers_for_hash(HASH_B) == {"w-1"}
    assert registry.get_workers_for_hash(HASH_C) == {"w-1"}


# ============================================================================
# Heartbeat & Dead Worker Eviction Tests
# ============================================================================


def test_record_heartbeat():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-1", used=1000)
    registry.register_worker(w1)

    hb_payload = HeartbeatPayload(
        worker_id="w-1",
        active_transfers=4,
        used_bytes=5000,
        inventory_delta_added={HASH_A},
    )
    success = registry.record_heartbeat("w-1", payload=hb_payload, current_time=1000.0)
    assert success is True

    worker = registry.get_worker("w-1")
    assert worker is not None
    assert worker.last_heartbeat_utc == 1000.0
    assert worker.active_transfers == 4
    assert worker.used_bytes == 5000
    assert HASH_A in worker.inventory_hashes


def test_evict_expired_workers():
    registry = WorkerRegistry(heartbeat_timeout_seconds=15.0)
    w1 = make_worker_info("w-alive", inventory={HASH_A})
    w2 = make_worker_info("w-dead", inventory={HASH_B})

    registry.register_worker(w1)
    registry.register_worker(w2)

    # Simulate w-alive heartbeat at t=100, w-dead at t=80
    registry.record_heartbeat("w-alive", current_time=100.0)
    registry.record_heartbeat("w-dead", current_time=80.0)

    # Eviction run at t=100 with 15s timeout (cutoff = 85.0)
    evicted = registry.evict_expired_workers(timeout_seconds=15.0, current_time=100.0)
    assert evicted == ["w-dead"]

    assert registry.has_worker("w-dead") is False
    assert registry.has_worker("w-alive") is True
    assert registry.get_workers_for_hash(HASH_B) == set()
    assert registry.get_workers_for_hash(HASH_A) == {"w-alive"}


# ============================================================================
# Health Scoring & Penalty Decay Tests
# ============================================================================


def test_penalty_lifecycle_and_corruption():
    registry = WorkerRegistry(degraded_threshold=3.0, suspect_threshold=10.0)
    w1 = make_worker_info("w-1", inventory={HASH_A, HASH_CORRUPT})
    registry.register_worker(w1)

    rec = registry.get_health_record("w-1")
    assert rec is not None
    assert rec.health_status == WorkerHealthStatus.HEALTHY
    assert rec.penalty_score == 0.0

    # Record 1 failure (+1.0)
    registry.record_failure("w-1", reason="timeout", penalty=1.0)
    rec = registry.get_health_record("w-1")
    assert rec.penalty_score == 1.0
    assert rec.health_status == WorkerHealthStatus.HEALTHY

    # Record Corruption event (+5.0) -> total 6.0 (degraded)
    registry.record_corruption("w-1", HASH_CORRUPT, penalty=5.0)
    rec = registry.get_health_record("w-1")
    assert rec.penalty_score == 6.0
    assert rec.total_corruptions == 1
    assert rec.health_status == WorkerHealthStatus.DEGRADED

    # Corrupt hash must have been eagerly pruned from worker inventory
    assert registry.get_workers_for_hash(HASH_CORRUPT) == set()

    # Success records decrease penalty (-0.5 each)
    for _ in range(7):  # 7 * 0.5 = 3.5 reduction -> 6.0 - 3.5 = 2.5 (< 3.0 -> HEALTHY)
        registry.record_success("w-1", bytes_transferred=1024)

    rec = registry.get_health_record("w-1")
    assert rec.penalty_score == 2.5
    assert rec.health_status == WorkerHealthStatus.HEALTHY
    assert rec.total_transfers_served == 7
    assert rec.total_bytes_served == 7 * 1024


def test_penalty_decay():
    registry = WorkerRegistry(penalty_half_life_seconds=100.0)
    w1 = make_worker_info("w-1")
    registry.register_worker(w1)

    registry.record_failure("w-1", penalty=8.0)
    rec = registry.get_health_record("w-1")
    assert rec.penalty_score == 8.0

    # Advance time by 1 half-life (100 seconds)
    t0 = rec.last_decay_utc
    registry.decay_penalties(current_time=t0 + 100.0)

    rec_decayed = registry.get_health_record("w-1")
    assert pytest.approx(rec_decayed.penalty_score, rel=1e-2) == 4.0


def test_cluster_stats():
    registry = WorkerRegistry()
    w1 = make_worker_info("w-1", capacity=1000, used=200, inventory={HASH_A})
    w2 = make_worker_info("w-2", capacity=2000, used=800, inventory={HASH_A, HASH_B})
    registry.register_worker(w1)
    registry.register_worker(w2)

    stats = registry.get_cluster_stats()
    assert stats.total_workers == 2
    assert stats.healthy_workers == 2
    assert stats.total_capacity_bytes == 3000
    assert stats.total_used_bytes == 1000
    assert stats.total_unique_hashes == 2


# ============================================================================
# Concurrency / Thread Safety Tests
# ============================================================================


def test_concurrent_registry_operations():
    registry = WorkerRegistry()
    num_threads = 8
    iterations_per_thread = 50

    def worker_task(thread_id: int):
        wid = f"worker-thread-{thread_id}"
        hashes = {f"{thread_id:02x}" + f"{i:02x}" * 31 for i in range(10)}
        info = make_worker_info(wid, inventory=hashes)
        registry.register_worker(info)

        for i in range(iterations_per_thread):
            registry.record_heartbeat(wid, current_time=time.time())
            registry.locate_hashes(list(hashes))
            if i % 5 == 0:
                registry.record_success(wid, 1024)
            if i % 10 == 0:
                registry.record_failure(wid, penalty=0.5)

    threads = [threading.Thread(target=worker_task, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert registry.get_worker_count() == num_threads
    stats = registry.get_cluster_stats()
    assert stats.total_workers == num_threads
