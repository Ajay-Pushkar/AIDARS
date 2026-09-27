from typing import Dict, Set
from aidars.distributed.cas_adapter import LocalCASAdapter

class AssetManager:
    """Generic asset handling layer.

    The adapter says: "These are my required assets."
    The generic asset layer handles hashing, deduplication, storage and synchronization.
    """

    def __init__(self, cas: LocalCASAdapter):
        self.cas = cas

    async def upload_assets(self, assets: Dict[str, bytes]) -> Set[str]:
        """Upload a collection of named assets and return their CAS hashes.

        Any storage failure from the CAS layer propagates to the caller
        rather than being swallowed, so a returned hash always corresponds
        to an asset that was actually committed to CAS.
        """
        hashes = set()
        for name, data in assets.items():
            stored_hash = self.cas.store_bytes(data)
            hashes.add(stored_hash)
        return hashes
