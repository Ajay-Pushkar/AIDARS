"""AIDAR Runtime Path Initializer.

Automatically ensures the src/ directory is on sys.path across all Python entrypoints.
"""
from pathlib import Path
import sys

src_path = Path(__file__).resolve().parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
