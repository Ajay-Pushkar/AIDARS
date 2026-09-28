from pathlib import Path
from typing import Dict, Set, Union
from aidars.distributed.cas_adapter import LocalCASAdapter

class AssetManager:
    """Generic asset handling layer.

    The adapter says: "These are my required assets."
    The generic asset layer handles hashing, deduplication, storage and synchronization.
    """

    def __init__(self, cas: LocalCASAdapter):
        self.cas = cas

    def upload_asset_file(self, source_path: Union[str, Path], expected_sha256: str) -> str:
        """Ingest a single asset directly from disk into CAS.

        Takes a physical file path (as already resolved by M4's
        PhysicalAssetResolver) and the SHA-256 M4 already computed for it,
        so the file is streamed straight into CAS via LocalCASAdapter's own
        chunked copy-and-verify path (store_file) instead of being read
        fully into memory and re-hashed. Any storage/verification failure
        propagates to the caller rather than being swallowed.
        """
        return self.cas.store_file(source_path, expected_sha256=expected_sha256)

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
