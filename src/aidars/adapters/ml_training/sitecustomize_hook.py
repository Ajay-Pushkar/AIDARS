"""M22: Zero-Code-Change sitecustomize.py Auto-Sharding Hook & Low-RAM Auto-Tuner.

Injected into the worker sandbox's sitecustomize.py so standard unmodified Python
ML training scripts (PyTorch DataLoader, scikit-learn, NumPy arrays, Voicebox/RVC)
automatically:
1. Lock CPU thread pools to the worker's optimal core allocation without oversubscribing.
2. Shard torch.utils.data.DataLoader to the PC's power-proportional dataset slice [start_ratio, end_ratio].
3. Auto-tune memory footprint (halve batch_size, disable pin_memory, set num_workers=0) on low-RAM (<3.5GB) nodes.
4. Materialize inline scripts as physical .py module files on disk so Windows multiprocessing
   / ProcessPoolExecutor (spawn start method) cleanly imports __main__ without BrokenProcessPool.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
from typing import Any, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


def configure_thread_pools(optimal_threads: Optional[int] = None) -> int:
    """Lock OpenMP, MKL, OpenBLAS, NumExpr, and PyTorch threads to optimal count."""
    if optimal_threads is None or optimal_threads <= 0:
        env_val = os.environ.get("AIDAR_OPTIMAL_THREADS")
        if env_val and env_val.isdigit():
            optimal_threads = int(env_val)
        else:
            cpus = os.cpu_count() or 4
            optimal_threads = max(1, cpus - 2) if cpus >= 8 else max(1, cpus - 1)

    t_str = str(optimal_threads)
    for var in [
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ]:
        os.environ[var] = t_str

    try:
        import torch
        torch.set_num_threads(optimal_threads)
    except Exception:
        pass

    return optimal_threads


def patch_torch_dataloader() -> bool:
    """Monkey-patch torch.utils.data.DataLoader for transparent power-proportional sharding and low-RAM micro-batching."""
    try:
        import torch
        import torch.utils.data
    except ImportError:
        return False

    if getattr(torch.utils.data.DataLoader, "_aidar_patched", False):
        return True

    _original_init = torch.utils.data.DataLoader.__init__

    def _patched_init(self, dataset, *args, **kwargs):
        # 1. Dataset Sharding based on power-proportional ratios
        try:
            start_ratio = float(os.environ.get("AIDAR_SHARD_START_RATIO", "0.0"))
            end_ratio = float(os.environ.get("AIDAR_SHARD_END_RATIO", "1.0"))
        except (ValueError, TypeError):
            start_ratio, end_ratio = 0.0, 1.0

        if (start_ratio > 0.0 or end_ratio < 1.0) and hasattr(dataset, "__len__"):
            try:
                total_len = len(dataset)
                if total_len >= 4:
                    i_start = int(round(total_len * start_ratio))
                    i_end = int(round(total_len * end_ratio))
                    i_start = max(0, min(total_len - 1, i_start))
                    i_end = max(i_start + 1, min(total_len, i_end))
                    dataset = torch.utils.data.Subset(dataset, range(i_start, i_end))
            except Exception as e:
                logger.warning("AIDAR DataLoader sharding failed: %s", e)

        # 2. Low-RAM Auto-Tuning (< 3.5 GB free RAM nodes)
        low_ram = os.environ.get("AIDAR_LOW_RAM_MODE", "0").strip() in ("1", "true", "yes")
        if low_ram:
            if "batch_size" in kwargs and kwargs["batch_size"] is not None:
                kwargs["batch_size"] = max(1, kwargs["batch_size"] // 2)
            elif len(args) >= 1 and args[0] is not None:
                args_list = list(args)
                args_list[0] = max(1, args_list[0] // 2)
                args = tuple(args_list)

            kwargs["pin_memory"] = False
            kwargs["num_workers"] = 0

        _original_init(self, dataset, *args, **kwargs)

    _patched_init._aidar_patched = True
    torch.utils.data.DataLoader.__init__ = _patched_init
    torch.utils.data.DataLoader._aidar_patched = True
    return True


def get_shard_range(total_count: int, start_ratio: Optional[float] = None, end_ratio: Optional[float] = None) -> Tuple[int, int]:
    """Calculate [start_idx, end_idx) slice for any list, dataset, or array."""
    if start_ratio is None:
        try:
            start_ratio = float(os.environ.get("AIDAR_SHARD_START_RATIO", "0.0"))
        except (ValueError, TypeError):
            start_ratio = 0.0
    if end_ratio is None:
        try:
            end_ratio = float(os.environ.get("AIDAR_SHARD_END_RATIO", "1.0"))
        except (ValueError, TypeError):
            end_ratio = 1.0

    if total_count <= 0:
        return 0, 0

    i_start = int(round(total_count * start_ratio))
    i_end = int(round(total_count * end_ratio))
    i_start = max(0, min(total_count, i_start))
    i_end = max(i_start, min(total_count, i_end))
    if i_end == i_start and total_count > 0:
        i_end = min(total_count, i_start + 1)
    return i_start, i_end


def shard_dataset(data: Sequence[Any]) -> Sequence[Any]:
    """Transparently slice a list, dataset, or numpy array by the worker's power share."""
    if not hasattr(data, "__len__"):
        return data
    total_len = len(data)
    if total_len < 4:
        return data
    i_start, i_end = get_shard_range(total_len)
    return data[i_start:i_end]


def generate_sitecustomize_code() -> str:
    """Return the self-contained sitecustomize.py code string to be written into sandboxes."""
    return (
        "# Auto-generated by AIDAR for hardware-adaptive ML execution\n"
        "import os, sys\n"
        "try:\n"
        "    from aidars.adapters.ml_training.sitecustomize_hook import configure_thread_pools, patch_torch_dataloader\n"
        "    configure_thread_pools()\n"
        "    patch_torch_dataloader()\n"
        "except Exception:\n"
        "    # Standalone inline fallback if aidars package is not directly on PYTHONPATH\n"
        "    cpus = os.cpu_count() or 4\n"
        "    opt_threads = max(1, cpus - 2) if cpus >= 8 else max(1, cpus - 1)\n"
        "    env_opt = os.environ.get('AIDAR_OPTIMAL_THREADS')\n"
        "    if env_opt and env_opt.isdigit():\n"
        "        opt_threads = int(env_opt)\n"
        "    t_str = str(opt_threads)\n"
        "    for var in ['OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'NUMEXPR_NUM_THREADS']:\n"
        "        os.environ[var] = t_str\n"
        "    try:\n"
        "        import torch\n"
        "        torch.set_num_threads(opt_threads)\n"
        "        import torch.utils.data\n"
        "        _orig_init = torch.utils.data.DataLoader.__init__\n"
        "        def _hooked_init(self, dataset, *args, **kwargs):\n"
        "            try:\n"
        "                sr = float(os.environ.get('AIDAR_SHARD_START_RATIO', '0.0'))\n"
        "                er = float(os.environ.get('AIDAR_SHARD_END_RATIO', '1.0'))\n"
        "            except Exception: sr, er = 0.0, 1.0\n"
        "            if (sr > 0.0 or er < 1.0) and hasattr(dataset, '__len__') and len(dataset) >= 4:\n"
        "                tl = len(dataset)\n"
        "                s = max(0, min(tl - 1, int(round(tl * sr))))\n"
        "                e = max(s + 1, min(tl, int(round(tl * er))))\n"
        "                dataset = torch.utils.data.Subset(dataset, range(s, e))\n"
        "            if os.environ.get('AIDAR_LOW_RAM_MODE', '0') == '1':\n"
        "                if 'batch_size' in kwargs and kwargs['batch_size']:\n"
        "                    kwargs['batch_size'] = max(1, kwargs['batch_size'] // 2)\n"
        "                kwargs['pin_memory'] = False\n"
        "                kwargs['num_workers'] = 0\n"
        "            _orig_init(self, dataset, *args, **kwargs)\n"
        "        torch.utils.data.DataLoader.__init__ = _hooked_init\n"
        "    except Exception:\n"
        "        pass\n"
    )


def materialize_script_for_execution(
    script_content_or_path: str,
    workdir: str,
    target_filename: str = "_user_ml_train.py",
) -> str:
    """Ensure ML script is materialized as a physical .py file and inject sitecustomize.py.

    This guarantees Windows multiprocessing / ProcessPoolExecutor (`spawn` method)
    can import __main__ cleanly without BrokenProcessPool.
    """
    os.makedirs(workdir, exist_ok=True)
    target_path = os.path.join(workdir, target_filename)

    # 1. Materialize user script file
    if os.path.exists(script_content_or_path) and os.path.isfile(script_content_or_path):
        if os.path.abspath(script_content_or_path) != os.path.abspath(target_path):
            shutil.copy2(script_content_or_path, target_path)
    else:
        # Treat as script source code
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(script_content_or_path)

    # 2. Inject sitecustomize.py in the working directory
    sitecustomize_path = os.path.join(workdir, "sitecustomize.py")
    with open(sitecustomize_path, "w", encoding="utf-8") as f:
        f.write(generate_sitecustomize_code())

    return target_path


# Automatically execute when sitecustomize is imported
if __name__ == "sitecustomize":
    configure_thread_pools()
    patch_torch_dataloader()

