"""
AIDARS Version Information.

Centralizes versioning information used throughout the Blender
Scene Intelligence subsystem.

Responsibilities
----------------
- Define the current AIDARS version.
- Define payload schema versions.
- Define supported Blender versions.
- Provide helper utilities for compatibility checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class Version:
    """
    Represents a semantic version.
    """

    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


# ==========================================================
# AIDARS
# ==========================================================

AIDARS_VERSION: Final = Version(
    major=1,
    minor=0,
    patch=0,
)

# ==========================================================
# Payload Schema
# ==========================================================

PAYLOAD_SCHEMA_VERSION: Final = Version(
    major=1,
    minor=0,
    patch=0,
)

# ==========================================================
# Blender Compatibility
# ==========================================================

MINIMUM_SUPPORTED_BLENDER: Final = Version(
    major=4,
    minor=5,
    patch=0,
)

TARGET_BLENDER: Final = Version(
    major=4,
    minor=5,
    patch=11,
)