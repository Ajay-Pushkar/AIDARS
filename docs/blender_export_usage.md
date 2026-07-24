# Blender export usage

There are two ways to get a .blend file into AIDARS. Both produce the same
JSON schema and go through the same CLI afterwards.

## Option A — Manual export (no configuration required)

1. Open the .blend file in Blender.
2. Open the Scripting workspace.
3. Open the script at `scripts/blender_export_scene.py`.
4. Run it.
5. The script writes `aidars_scene_payload.json` next to the .blend file.
6. Run the CLI with that JSON file:

```bash
python -m aidars.scene_intelligence.cli aidars_scene_payload.json -o output/scene.json
```

## Option B — Automatic export via an external Blender executable

If a Blender executable is available on the machine running AIDARS (not
necessarily the artist's machine), `BlenderAdapter` can invoke it directly in
background mode and skip the manual export step:

```python
from aidars.scene_intelligence.blender_adapter import BlenderAdapter

adapter = BlenderAdapter(blender_executable="/path/to/blender")
scene_data = adapter.load_scene("my_scene.blend")
```

Internally this runs:

```bash
blender --background my_scene.blend --python src/aidars/scene_intelligence/blender_scripts/inspect_scene.py
```

`inspect_scene.py` runs inside Blender's own Python interpreter and delegates
to `scene_metadata.py`, `collection_extractor.py`, and `object_extractor.py`
(all under `blender_scripts/`) to build the JSON payload, then prints it to
stdout for `BlenderAdapter` to capture. If no Blender executable is
configured, or the embedded/external inspection path raises for any reason,
`BlenderAdapter` falls back to a minimal placeholder payload rather than
failing outright — check application logs for a warning when that happens.

## Every run also produces a dependency graph report

Whichever export path you use, the CLI always builds a dependency graph
alongside the scene snapshot (unless you pass `--no-graph`):

```bash
python -m aidars.scene_intelligence.cli aidars_scene_payload.json \
    -o output/scene.json \
    --graph-output output/dependency_graph.json
```

`output/dependency_graph.json` contains the graph's nodes/edges plus an
`integrity` section listing any `missing_targets` (dangling references, e.g.
a constraint pointing at a deleted object) and `unused_nodes` (assets nothing
in the scene references, e.g. an empty collection). The CLI also prints a
short warning to stderr for either case so problems surface immediately
without having to open the JSON.

