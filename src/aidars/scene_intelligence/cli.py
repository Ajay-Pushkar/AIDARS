from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import SceneIntelligenceEngine
from .exporters import JsonSceneExporter
from .loader import SceneLoader
from .blender_adapter import BlenderAdapter
from aidars.smart_package.builder import PackageAsset, SmartPackageBuilder


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for scene intelligence."""

    parser = argparse.ArgumentParser(description="Analyze a scene payload, Blender-exported JSON file, or .blend input")
    parser.add_argument("input_path", help="Path to a JSON scene payload file or .blend file")
    parser.add_argument("-o", "--output", default="output/scene.json", help="Output JSON file path")
    parser.add_argument("--package", action="store_true", help="Also emit a smart packaging manifest")
    parser.add_argument("--frame-start", type=int, default=1, help="First frame for packaging")
    parser.add_argument("--frame-end", type=int, default=24, help="Last frame for packaging")
    parser.add_argument("--package-output", default="output/package.json", help="Path to write the package manifest")
    return parser


def load_scene_payload(input_path: str | Path) -> dict[str, Any]:
    """Load a scene payload from JSON on disk or via Blender adapter."""

    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() == ".blend":
        adapter = BlenderAdapter()
        return adapter.load_scene(path)

    with path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise TypeError("Scene payload must be a JSON object")

    return data


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        payload = load_scene_payload(args.input_path)
        engine = SceneIntelligenceEngine()
        snapshot = engine.analyze_scene_data(payload)
        output_path = JsonSceneExporter.write_json(snapshot, args.output)
        print(f"Scene snapshot written to {output_path}")

        if args.package:
            assets = []
            if isinstance(payload, dict) and isinstance(payload.get("assets"), list):
                assets = [
                    PackageAsset(path=asset.get("path", ""), kind=asset.get("kind", "unknown"), size_bytes=int(asset.get("size_bytes", 0)))
                    for asset in payload.get("assets", [])
                    if isinstance(asset, dict)
                ]
            builder = SmartPackageBuilder()
            manifest = builder.build_package(args.frame_start, args.frame_end, assets)
            package_path = builder.write_manifest(manifest, args.package_output)
            print(f"Package manifest written to {package_path}")
        return 0
    except (FileNotFoundError, ValueError, TypeError, json.JSONDecodeError) as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
