import unittest


class PackageImportTests(unittest.TestCase):
    def test_imports_work_from_repository_root(self) -> None:
        import aidars
        import aidars.adapters.blender.intelligence

        self.assertTrue(hasattr(aidars, "__file__"))
        self.assertTrue(hasattr(aidars.adapters.blender.intelligence, "__file__"))


if __name__ == "__main__":
    unittest.main()
