"""M22: Universal Federated Model Merger & Feature Aggregator.

Merges multi-worker ML training outputs:
1. Power-Weighted / Sample-Weighted Federated Averaging (FedAvg / DiLoCo Model Soup)
   for neural network checkpoints (.pt, .pth, .safetensors, .npz, .json weights).
2. Feature artifact aggregation and concatenation for Voicebox / RVC voice cloning
   and image LoRA preprocessing (.wav, .f0.npy, .npy feature matrices, .index).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

logger = logging.getLogger(__name__)


def _normalize_weights(
    worker_keys: Sequence[str],
    worker_weights: Optional[Union[Dict[str, float], Sequence[float]]] = None,
) -> Dict[str, float]:
    """Compute normalized weights sum(w_i) = 1.0."""
    n = len(worker_keys)
    if n == 0:
        return {}
    if worker_weights is None:
        return {k: 1.0 / n for k in worker_keys}

    weights_dict: Dict[str, float] = {}
    if isinstance(worker_weights, dict):
        for k in worker_keys:
            weights_dict[k] = float(worker_weights.get(k, 1.0))
    elif isinstance(worker_weights, (list, tuple)):
        for idx, k in enumerate(worker_keys):
            w = float(worker_weights[idx]) if idx < len(worker_weights) else 1.0
            weights_dict[k] = w
    else:
        return {k: 1.0 / n for k in worker_keys}

    total_w = sum(weights_dict.values())
    if total_w <= 0.0:
        return {k: 1.0 / n for k in worker_keys}
    return {k: v / total_w for k, v in weights_dict.items()}


def _fedavg_pytorch_state_dict(
    state_dicts: Sequence[Dict[str, Any]],
    normalized_weights: Sequence[float],
) -> Dict[str, Any]:
    """Compute sample-weighted average across PyTorch state dictionaries."""
    try:
        import torch
    except ImportError:
        raise RuntimeError("PyTorch is required to merge .pt/.pth checkpoints")

    if not state_dicts:
        return {}
    if len(state_dicts) == 1:
        return state_dicts[0]

    merged: Dict[str, Any] = {}
    ref = state_dicts[0]

    for k, v in ref.items():
        if isinstance(v, torch.Tensor):
            if torch.is_floating_point(v) or torch.is_complex(v):
                accum = torch.zeros_like(v, dtype=torch.float32)
                for sd, weight in zip(state_dicts, normalized_weights):
                    cur_tensor = sd.get(k)
                    if cur_tensor is not None:
                        accum += float(weight) * cur_tensor.to(torch.float32)
                merged[k] = accum.to(v.dtype)
            else:
                # Integer/boolean metadata tensors (e.g. num_batches_tracked)
                # For step/epoch counters or batch trackers, sum or take rank 0
                if "batches_tracked" in k or "step" in k or "count" in k:
                    total_count = sum(sd[k].item() for sd in state_dicts if k in sd)
                    merged[k] = torch.tensor(total_count, dtype=v.dtype, device=v.device)
                else:
                    merged[k] = v.clone()
        elif isinstance(v, dict):
            sub_sds = [sd.get(k, {}) for sd in state_dicts]
            merged[k] = _fedavg_pytorch_state_dict(sub_sds, normalized_weights)
        else:
            merged[k] = v

    return merged


def _fedavg_numpy_dict(
    arrays_dict_list: Sequence[Dict[str, Any]],
    normalized_weights: Sequence[float],
) -> Dict[str, Any]:
    """Compute sample-weighted average across NumPy array dictionaries (.npz)."""
    import numpy as np

    if not arrays_dict_list:
        return {}
    if len(arrays_dict_list) == 1:
        return dict(arrays_dict_list[0])

    merged: Dict[str, Any] = {}
    ref = arrays_dict_list[0]

    for k, v in ref.items():
        arr = np.asarray(v)
        if np.issubdtype(arr.dtype, np.floating) or np.issubdtype(arr.dtype, np.complexfloating):
            accum = np.zeros_like(arr, dtype=np.float64)
            for ad, weight in zip(arrays_dict_list, normalized_weights):
                cur_arr = np.asarray(ad[k])
                accum += float(weight) * cur_arr.astype(np.float64)
            merged[k] = accum.astype(arr.dtype)
        else:
            merged[k] = arr.copy()

    return merged


def _fedavg_json_dict(
    json_dicts: Sequence[Dict[str, Any]],
    normalized_weights: Sequence[float],
) -> Dict[str, Any]:
    """Compute weighted average for numerical JSON dictionaries."""
    if not json_dicts:
        return {}
    if len(json_dicts) == 1:
        return json_dicts[0]

    merged: Dict[str, Any] = {}
    ref = json_dicts[0]

    for k, v in ref.items():
        if isinstance(v, (int, float)):
            merged[k] = sum(float(jd.get(k, v)) * w for jd, w in zip(json_dicts, normalized_weights))
        elif isinstance(v, list) and v and isinstance(v[0], (int, float)):
            length = len(v)
            accum_list = [0.0] * length
            for jd, w in zip(json_dicts, normalized_weights):
                cur_list = jd.get(k, v)
                for idx in range(min(length, len(cur_list))):
                    accum_list[idx] += float(cur_list[idx]) * w
            merged[k] = accum_list
        elif isinstance(v, dict):
            merged[k] = _fedavg_json_dict([jd.get(k, {}) for jd in json_dicts], normalized_weights)
        else:
            merged[k] = v

    return merged


def merge_worker_ml_outputs(
    worker_outputs: Union[Sequence[Union[str, Path]], Dict[str, Union[str, Path]]],
    dest_dir: Union[str, Path],
    worker_weights: Optional[Union[Dict[str, float], Sequence[float]]] = None,
) -> Dict[str, str]:
    """Merge checkpoints and aggregate features from all participating workers.

    Parameters:
    - worker_outputs: List of directory paths or Dict of {worker_id: directory_path}
    - dest_dir: Destination directory for merged models and aggregated features
    - worker_weights: Optional throughput scores or sample counts for FedAvg

    Returns:
    - Dict mapping filename -> destination file path
    """
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    # 1. Normalize worker mapping
    worker_map: Dict[str, Path] = {}
    if isinstance(worker_outputs, dict):
        for k, v in worker_outputs.items():
            p = Path(v)
            if p.exists():
                worker_map[str(k)] = p
    else:
        for idx, p_item in enumerate(worker_outputs):
            p = Path(p_item)
            if p.exists():
                worker_map[f"worker_{idx}"] = p

    if not worker_map:
        logger.warning("No valid worker output directories provided to merge_worker_ml_outputs")
        return {}

    worker_keys = list(worker_map.keys())
    normalized_weights_dict = _normalize_weights(worker_keys, worker_weights)
    normalized_weights = [normalized_weights_dict[k] for k in worker_keys]

    # 2. Index all files produced across workers
    # filename -> dict of {worker_id: file_path}
    files_by_name: Dict[str, Dict[str, Path]] = {}
    for wid, w_dir in worker_map.items():
        if w_dir.is_file():
            files_by_name.setdefault(w_dir.name, {})[wid] = w_dir
            continue
        for root, _, files in os.walk(w_dir):
            for f in files:
                rel_path = os.path.relpath(os.path.join(root, f), w_dir)
                files_by_name.setdefault(rel_path, {})[wid] = Path(root) / f

    merged_results: Dict[str, str] = {}

    # 3. Process files by extension / type
    for rel_name, w_files in files_by_name.items():
        out_dest = dest_path / rel_name
        out_dest.parent.mkdir(parents=True, exist_ok=True)
        ext = out_dest.suffix.lower()

        # A. PyTorch Checkpoints (.pt, .pth)
        if ext in (".pt", ".pth"):
            try:
                import torch
                loaded_sds = []
                loaded_weights = []
                is_full_checkpoint = False
                outer_dict_ref = None

                for wid in worker_keys:
                    if wid in w_files:
                        loaded = torch.load(w_files[wid], map_location="cpu", weights_only=False)
                        if isinstance(loaded, dict) and "state_dict" in loaded:
                            is_full_checkpoint = True
                            outer_dict_ref = loaded.copy()
                            loaded_sds.append(loaded["state_dict"])
                        elif isinstance(loaded, dict) and "model" in loaded and isinstance(loaded["model"], dict):
                            is_full_checkpoint = True
                            outer_dict_ref = loaded.copy()
                            loaded_sds.append(loaded["model"])
                        elif isinstance(loaded, dict):
                            loaded_sds.append(loaded)
                        loaded_weights.append(normalized_weights_dict[wid])

                # Renormalize active subset weights
                sum_w = sum(loaded_weights) or 1.0
                active_norm_weights = [w / sum_w for w in loaded_weights]

                merged_sd = _fedavg_pytorch_state_dict(loaded_sds, active_norm_weights)
                if is_full_checkpoint and outer_dict_ref is not None:
                    if "state_dict" in outer_dict_ref:
                        outer_dict_ref["state_dict"] = merged_sd
                    else:
                        outer_dict_ref["model"] = merged_sd
                    torch.save(outer_dict_ref, str(out_dest))
                else:
                    torch.save(merged_sd, str(out_dest))

                merged_results[rel_name] = str(out_dest)
                continue
            except Exception as e:
                logger.warning("PyTorch checkpoint FedAvg failed for %s: %s; falling back to copy", rel_name, e)

        # B. NumPy NPZ Checkpoints (.npz)
        if ext == ".npz":
            try:
                import numpy as np
                loaded_dicts = []
                loaded_weights = []
                for wid in worker_keys:
                    if wid in w_files:
                        with np.load(w_files[wid]) as npz:
                            loaded_dicts.append({k: npz[k] for k in npz.files})
                        loaded_weights.append(normalized_weights_dict[wid])

                sum_w = sum(loaded_weights) or 1.0
                active_norm_weights = [w / sum_w for w in loaded_weights]
                merged_dict = _fedavg_numpy_dict(loaded_dicts, active_norm_weights)
                np.savez_compressed(str(out_dest), **merged_dict)
                merged_results[rel_name] = str(out_dest)
                continue
            except Exception as e:
                logger.warning("NPZ FedAvg failed for %s: %s", rel_name, e)

        # C. SafeTensors Checkpoints (.safetensors)
        if ext == ".safetensors":
            try:
                from safetensors.numpy import load_file, save_file
                loaded_dicts = []
                loaded_weights = []
                for wid in worker_keys:
                    if wid in w_files:
                        loaded_dicts.append(load_file(str(w_files[wid])))
                        loaded_weights.append(normalized_weights_dict[wid])

                sum_w = sum(loaded_weights) or 1.0
                active_norm_weights = [w / sum_w for w in loaded_weights]
                merged_dict = _fedavg_numpy_dict(loaded_dicts, active_norm_weights)
                save_file(merged_dict, str(out_dest))
                merged_results[rel_name] = str(out_dest)
                continue
            except Exception as e:
                logger.warning("SafeTensors FedAvg failed for %s: %s", rel_name, e)

        # D. JSON weights / metrics (.json)
        if ext == ".json":
            try:
                loaded_jsons = []
                loaded_weights = []
                for wid in worker_keys:
                    if wid in w_files:
                        with open(w_files[wid], "r", encoding="utf-8") as f:
                            loaded_jsons.append(json.load(f))
                        loaded_weights.append(normalized_weights_dict[wid])
                if loaded_jsons and isinstance(loaded_jsons[0], dict):
                    sum_w = sum(loaded_weights) or 1.0
                    active_norm_weights = [w / sum_w for w in loaded_weights]
                    merged_json = _fedavg_json_dict(loaded_jsons, active_norm_weights)
                    with open(out_dest, "w", encoding="utf-8") as f:
                        json.dump(merged_json, f, indent=2)
                    merged_results[rel_name] = str(out_dest)
                    continue
            except Exception as e:
                logger.debug("JSON FedAvg skipped for %s: %s", rel_name, e)

        # E. Feature concatenation: total_fea.npy / features.npy for Voicebox/RVC
        if ext == ".npy" and ("total_fea" in rel_name.lower() or "features" in rel_name.lower()):
            try:
                import numpy as np
                arrays = [np.load(w_files[wid]) for wid in worker_keys if wid in w_files]
                if arrays:
                    concatenated = np.concatenate(arrays, axis=0)
                    np.save(str(out_dest), concatenated)
                    merged_results[rel_name] = str(out_dest)
                    continue
            except Exception as e:
                logger.warning("Feature concatenation failed for %s: %s", rel_name, e)

        # F. Audio slices, pitch files (.wav, .f0.npy), indices, or individual files
        # If file only exists on one worker, copy it over cleanly
        if len(w_files) == 1:
            src_f = next(iter(w_files.values()))
            shutil.copy2(src_f, out_dest)
            merged_results[rel_name] = str(out_dest)
        else:
            # Multiple workers produced the same non-model file (e.g. slice_0.wav)
            # Copy each with worker discriminator suffix to avoid overwrite
            for wid, src_f in w_files.items():
                stem = out_dest.stem
                suff = out_dest.suffix
                distinct_name = f"{stem}_{wid}{suff}"
                distinct_dest = out_dest.parent / distinct_name
                shutil.copy2(src_f, distinct_dest)
                merged_results[distinct_name] = str(distinct_dest)

    logger.info("Successfully merged %d artifact(s) into %s", len(merged_results), dest_dir)
    return merged_results

