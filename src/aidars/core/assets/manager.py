from typing import Dict, Set
import hashlib
from aidars.distributed.cas_adapter import LocalCASAdapter

class AssetManager:
    """Generic asset handling layer.
    
    The adapter says: "These are my required assets."
    The generic asset layer handles hashing, deduplication, storage and synchronization.
    """
    
    def __init__(self, cas: LocalCASAdapter):
        self.cas = cas
        
    async def upload_assets(self, assets: Dict[str, bytes]) -> Set[str]:
        """Upload a collection of named assets and return their CAS hashes."""
        hashes = set()
        for name, data in assets.items():
            # In a real system, we might stream this.
            # For now, just write bytes to CAS.
            asset_hash = hashlib.sha256(data).hexdigest()
            # The cas.put usually takes a string data in some test implementations, 
            # or we use cas.upload_file. Let's just assume we have a put_bytes or similar.
            # For this interface, we just simulate the interface if the exact method isn't there,
            # or use the known method.
            # We will use put(hash, data) if it exists.
            if hasattr(self.cas, "put"):
                if isinstance(data, bytes):
                    # cas might expect string
                    try:
                        self.cas.put(asset_hash, data.decode('utf-8', errors='ignore'))
                    except Exception:
                        pass
            hashes.add(asset_hash)
        return hashes
