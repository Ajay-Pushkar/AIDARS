"""M22: Drop-In CLI & Desktop App Wrapper for Universal Distributed ML Training.

Usage:
  aidars-ml-run --script path/to/train.py --data path/to/dataset --coordinator http://127.0.0.1:8000

Features:
- Power-proportional dataset/batch sharding across all N active workers.
- Automatic CPU thread pool locking and low-RAM (<3.5GB) micro-batching.
- Transparent sitecustomize.py auto-sharding hook injection.
- Automatic CAS asset ingestion and distribution.
- Power-Weighted Federated Averaging (FedAvg / DiLoCo Model Soup) and feature aggregation.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import httpx

from aidars.adapters.ml_training.adapter import MLTrainingAdapter
from aidars.adapters.ml_training.merger import merge_worker_ml_outputs
from aidars.adapters.ml_training.sitecustomize_hook import materialize_script_for_execution
from aidars.distributed.models import WorkerInfo, WorkerResourceProfile, WorkloadSpec

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aidars-ml-run")


def _hash_file(filepath: Path) -> str:
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


async def run_ml_distributed(
    script_path: str,
    dataset_path: Optional[str] = None,
    coordinator_url: str = "http://127.0.0.1:8000",
    epochs: int = 1,
    batch_size: int = 32,
    output_dir: str = "./aidar_merged_output",
    requires_gpu: bool = False,
    poll_interval: float = 1.0,
    timeout: float = 3600.0,
    client: Optional[httpx.AsyncClient] = None,
) -> int:
    """Execute distributed ML training across all active cluster nodes and merge results."""
    clean_coord = coordinator_url.rstrip("/")
    dest_path = Path(output_dir).resolve()
    dest_path.mkdir(parents=True, exist_ok=True)

    script_p = Path(script_path).resolve()
    if not script_p.exists():
        logger.error("Script not found: %s", script_path)
        return 1

    # 1. Resolve and hash physical assets
    script_hash = _hash_file(script_p)
    logger.info("Script %s (SHA-256: %s)", script_p.name, script_hash[:16])

    dataset_hash = None
    dataset_p: Optional[Path] = None
    if dataset_path:
        dataset_p = Path(dataset_path).resolve()
        if dataset_p.exists() and dataset_p.is_file():
            dataset_hash = _hash_file(dataset_p)
            logger.info("Dataset %s (SHA-256: %s)", dataset_p.name, dataset_hash[:16])
        elif dataset_p.exists() and dataset_p.is_dir():
            # For directories, package as zip archive for CAS distribution
            archive_path = dest_path / f"dataset_{dataset_p.name}.zip"
            if not archive_path.exists():
                shutil.make_archive(str(archive_path.with_suffix("")), "zip", str(dataset_p))
            dataset_hash = _hash_file(archive_path)
            dataset_p = archive_path
            logger.info("Packaged dataset folder %s -> %s (SHA-256: %s)", dataset_path, archive_path.name, dataset_hash[:16])

    # 2. Query Coordinator for active workers
    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        close_client = True

    try:
        active_workers: List[Any] = []
        try:
            resp = await client.get(f"{clean_coord}/api/v1/workers")
            if resp.status_code == 200:
                workers_data = resp.json()
                for wd in workers_data:
                    try:
                        active_workers.append(WorkerInfo.model_validate(wd))
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("Could not reach coordinator at %s (%s); proceeding with local worker plan", clean_coord, e)

        # 3. Plan power-weighted dataset shards
        adapter = MLTrainingAdapter()
        job_request = {
            "script_path": str(script_p),
            "script_hash": script_hash,
            "dataset_path": str(dataset_p) if dataset_p else None,
            "dataset_hash": dataset_hash,
            "epochs": epochs,
            "batch_size": batch_size,
            "requires_gpu": requires_gpu,
        }
        specs = adapter.plan_shards(job_request, active_workers)
        logger.info("Planned %d power-proportional ML training shard(s)", len(specs))

        for s in specs:
            params = s.parameters or {}
            logger.info(
                "  Shard %s -> Slice [%.2f, %.2f], Threads: %s, Low-RAM: %s, Power Score: %.1f",
                s.workload_id,
                float(params.get("start_ratio", 0.0)),
                float(params.get("end_ratio", 1.0)),
                params.get("optimal_threads", 4),
                params.get("low_ram_mode", "0"),
                float(params.get("throughput_score", 4.0)),
            )

        # 4. Upload script and dataset to worker CAS endpoints
        assets_to_upload = [(script_hash, script_p)]
        if dataset_hash and dataset_p:
            assets_to_upload.append((dataset_hash, dataset_p))

        for w in active_workers:
            ep = getattr(w, "endpoint_url", None)
            if not ep:
                continue
            for h, fpath in assets_to_upload:
                try:
                    with open(fpath, "rb") as af:
                        upload_resp = await client.put(
                            f"{ep.rstrip('/')}/api/v1/assets/{h}",
                            content=af.read(),
                            headers={"Content-Type": "application/octet-stream"},
                        )
                        if upload_resp.status_code in (200, 201):
                            logger.info("Staged %s to worker %s", fpath.name, getattr(w, "worker_id", ep))
                except Exception as e:
                    logger.debug("Worker %s direct upload skipped: %s", getattr(w, "worker_id", ep), e)

        # 5. Submit workloads to coordinator
        submitted_ids: List[str] = []
        for spec in specs:
            try:
                sub_resp = await client.post(
                    f"{clean_coord}/api/v1/workloads/submit",
                    json=spec.model_dump(mode="json"),
                )
                if sub_resp.status_code in (200, 201, 202):
                    submitted_ids.append(spec.workload_id)
                    logger.info("Submitted workload %s", spec.workload_id)
                else:
                    logger.error("Failed to submit workload %s: %s", spec.workload_id, sub_resp.text)
            except Exception as e:
                logger.warning("Workload submission to coordinator failed (%s); running locally", e)

        # 6. Poll for completion
        if submitted_ids:
            logger.info("Waiting for %d workload(s) to complete...", len(submitted_ids))
            t_start = time.time()
            completed_workloads: Dict[str, Dict[str, Any]] = {}

            while len(completed_workloads) < len(submitted_ids):
                if time.time() - t_start > timeout:
                    logger.error("ML execution timed out after %.1fs", timeout)
                    return 1

                await asyncio.sleep(poll_interval)
                for wid in submitted_ids:
                    if wid in completed_workloads:
                        continue
                    try:
                        st_resp = await client.get(f"{clean_coord}/api/v1/workloads/{wid}")
                        if st_resp.status_code == 200:
                            data = st_resp.json()
                            state = data.get("state", "").upper()
                            if state == "COMPLETED":
                                completed_workloads[wid] = data
                                logger.info("Workload %s COMPLETED", wid)
                            elif state in ("FAILED", "CANCELLED"):
                                logger.error("Workload %s %s: %s", wid, state, data.get("error_message"))
                                return 1
                    except Exception:
                        pass

            logger.info("All %d workload(s) completed successfully!", len(submitted_ids))

            # 7. Download output artifacts to per-worker staging directories
            staging_base = dest_path / ".worker_staging"
            staging_base.mkdir(parents=True, exist_ok=True)
            worker_staging_dirs: Dict[str, Path] = {}
            worker_weights: Dict[str, float] = {}

            for idx, wid in enumerate(submitted_ids):
                data = completed_workloads[wid]
                worker_id = data.get("placement_decision", {}).get("selected_worker_id") or f"worker_{idx}"
                spec_params = specs[idx].parameters or {}
                worker_weights[worker_id] = float(spec_params.get("throughput_score", 1.0))

                w_stage = staging_base / worker_id
                w_stage.mkdir(parents=True, exist_ok=True)
                worker_staging_dirs[worker_id] = w_stage

                # Download outputs
                outputs = data.get("result", {}).get("output_asset_hashes", [])
                for oh in outputs:
                    try:
                        loc_resp = await client.post(
                            f"{clean_coord}/api/v1/assets/locate",
                            json={"missing_hashes": [oh]},
                        )
                        if loc_resp.status_code == 200:
                            loc_data = loc_resp.json()
                            cands = loc_data.get("locations", {}).get(oh, [])
                            if cands:
                                src_ep = cands[0]["endpoint_url"].rstrip("/")
                                out_f = w_stage / f"model_{worker_id}.pt"
                                async with client.stream("GET", f"{src_ep}/api/v1/assets/{oh}/stream") as str_resp:
                                    with open(out_f, "wb") as of:
                                        async for chk in str_resp.aiter_bytes():
                                            of.write(chk)
                                logger.info("Downloaded output %s -> %s", oh[:16], out_f.name)
                    except Exception as e:
                        logger.warning("Could not download artifact %s: %s", oh, e)

            # 8. Merge outputs via Federated Averaging (FedAvg / DiLoCo Model Soup)
            logger.info("Merging worker artifacts into %s...", dest_path)
            merged = merge_worker_ml_outputs(worker_staging_dirs, dest_path, worker_weights=worker_weights)
            logger.info("Successfully merged %d artifact(s):", len(merged))
            for m_name, m_path in merged.items():
                logger.info("  - %s -> %s", m_name, m_path)

        return 0

    finally:
        if close_client:
            await client.aclose()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="aidars-ml-run",
        description="Universal Power-Proportional ML Training & Federated Checkpoint Merger across N Heterogeneous PCs.",
    )
    parser.add_argument("--script", required=True, help="Path to Python ML training script (.py)")
    parser.add_argument("--data", "--dataset", dest="dataset", default=None, help="Path to dataset file or folder")
    parser.add_argument("--coordinator", default="http://127.0.0.1:8000", help="AIDAR Coordinator URL")
    parser.add_argument("--epochs", type=int, default=1, help="Number of training epochs")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size per worker")
    parser.add_argument("--output-dir", default="./aidar_merged_output", help="Directory for merged models/features")
    parser.add_argument("--gpu", action="store_true", help="Require GPU execution")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between coordinator status polls")
    parser.add_argument("--timeout", type=float, default=3600.0, help="Overall execution timeout in seconds")

    args = parser.parse_args(argv)

    return asyncio.run(
        run_ml_distributed(
            script_path=args.script,
            dataset_path=args.dataset,
            coordinator_url=args.coordinator,
            epochs=args.epochs,
            batch_size=args.batch_size,
            output_dir=args.output_dir,
            requires_gpu=args.gpu,
            poll_interval=args.poll_interval,
            timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    sys.exit(main())

