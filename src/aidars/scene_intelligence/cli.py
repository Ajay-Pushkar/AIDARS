"""Thin CLI wrapper around SceneEngine.

Per the architecture: the CLI's only responsibilities are (1) parse
arguments, (2) invoke the Scene Engine, (3) exit. All business logic -
loading, analysis, dependency graph construction, integrity checking,
packaging, caching - lives in SceneEngine so it stays reusable from tests,
a future API, or a future GUI without going through argparse at all.
"""
from __future__ import annotations

import argparse
import json
import sys

from .scene_engine import SceneEngine, SceneEngineRequest


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for scene intelligence."""

    parser = argparse.ArgumentParser(description="Analyze a scene payload, Blender-exported JSON file, or .blend input")
    parser.add_argument("input_path", help="Path to a JSON scene payload file or .blend file")
    parser.add_argument("-o", "--output", default="output/scene.json", help="Output JSON file path")
    parser.add_argument(
        "--graph-output",
        default="output/dependency_graph.json",
        help="Path to write the dependency graph + asset-integrity report",
    )
    parser.add_argument(
        "--no-graph",
        action="store_true",
        help="Skip building and writing the dependency graph report",
    )
    parser.add_argument("--package", action="store_true", help="Also emit a smart packaging manifest")
    parser.add_argument(
        "--optimize-package-by-visibility",
        action="store_true",
        help=(
            "When combined with --package: prune packaged assets to only those "
            "reachable from objects visible in --frame-start/--frame-end, via the "
            "dependency graph. Off by default."
        ),
    )
    parser.add_argument("--frame-start", type=int, default=1, help="First frame for packaging")
    parser.add_argument("--frame-end", type=int, default=24, help="Last frame for packaging")
    parser.add_argument("--package-output", default="output/package.json", help="Path to write the package manifest")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help=(
            "Enable incremental scanning: skip re-analysis and reuse the previous "
            "outputs when the input's content hash hasn't changed since the last "
            "run. Disabled by default; pass a directory (e.g. .aidars_cache) to enable."
        ),
    )
    parser.add_argument(
        "--blender-executable",
        default=None,
        help="Path to a Blender executable, for the automatic .blend export path",
    )
    return parser


def _request_from_args(args: argparse.Namespace) -> SceneEngineRequest:
    """Translate parsed CLI arguments into a SceneEngineRequest.

    This is the entire CLI-to-business-logic boundary: everything past
    this point is SceneEngine's responsibility, not the CLI's.
    """
    return SceneEngineRequest(
        input_path=args.input_path,
        scene_output=args.output,
        graph_output=args.graph_output,
        build_graph=not args.no_graph,
        build_package=args.package,
        optimize_package_by_visibility=args.optimize_package_by_visibility,
        frame_start=args.frame_start,
        frame_end=args.frame_end,
        package_output=args.package_output,
        cache_dir=args.cache_dir,
        blender_executable=args.blender_executable,
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: parse arguments, invoke the Scene Engine, exit."""

    parser = build_parser()
    args = parser.parse_args(argv)
    request = _request_from_args(args)

    try:
        engine = SceneEngine(blender_executable=request.blender_executable)
        result = engine.run(request)
    except (FileNotFoundError, ValueError, TypeError, json.JSONDecodeError) as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    for message in result.messages:
        print(message)
    for warning in result.warnings:
        print(warning, file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
