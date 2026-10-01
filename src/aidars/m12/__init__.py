"""M12 advisory intelligence built on AIDAR's existing M7 and durable ledgers."""

from aidars.m12.history import HistoricalObservationProjector
from aidars.m12.service import DistributedIntelligence

__all__ = ["DistributedIntelligence", "HistoricalObservationProjector"]
