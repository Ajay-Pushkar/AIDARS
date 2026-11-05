"""PyTorch DDP (DistributedDataParallel) Adapter for Heterogeneous N-PC clusters."""
from aidars.adapters.pytorch_ddp.adapter import (
    PyTorchDDPAdapter,
    PowerWeightedDistributedSampler,
    detect_lan_network_interface,
)

__all__ = [
    "PyTorchDDPAdapter",
    "PowerWeightedDistributedSampler",
    "detect_lan_network_interface",
]

