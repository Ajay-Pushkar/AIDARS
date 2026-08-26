import unittest
import tempfile
import os
from pathlib import Path
from aidars.cache.store import DiskCacheStore

class TestReconciliation(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self.tmp_dir.name)
        
    def tearDown(self):
        self.tmp_dir.cleanup()
        
    def test_reconcile_dangling_and_orphans(self):
        with DiskCacheStore(self.cache_dir) as store:
            # Add an entry normally
            entry1 = store.put_bytes(b"valid data")
            
            # Create a dangling entry (in db, not on disk)
            store.index.put(entry1) # Put again just to have it
            store.storage.delete(entry1.sha256) # Delete physical file
            
            # Create an orphan file (on disk, not in db)
            dest, size, computed = store.storage.write_bytes(b"orphan data")
            
            # Reconcile
            report = store.reconcile()
            
            self.assertEqual(report["dangling_removed"], 1)
            self.assertEqual(report["orphans_removed"], 1)
            self.assertEqual(report["corrupted_removed"], 0)
            
            # Verify they are gone
            self.assertFalse(store.index.contains(entry1.sha256))
            self.assertFalse(store.storage.exists(computed))

    def test_reconcile_corrupted(self):
        with DiskCacheStore(self.cache_dir) as store:
            entry1 = store.put_bytes(b"corrupt me")
            
            # Corrupt the file on disk by modifying its contents directly
            with open(store.get_path(entry1.sha256), "wb") as f:
                f.write(b"bad")
                
            report = store.reconcile(verify_hashes=True)
            
            self.assertEqual(report["corrupted_removed"], 1)
            self.assertFalse(store.contains(entry1.sha256))

if __name__ == "__main__":
    unittest.main()
