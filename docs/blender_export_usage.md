# Blender export usage

To export a Blender scene so AIDARS can analyze it:

1. Open the .blend file in Blender.
2. Open the Scripting workspace.
3. Open the script at `scripts/blender_export_scene.py`.
4. Run it.
5. The script writes `aidars_scene_payload.json` next to the .blend file.
6. Run the CLI with that JSON file:

```bash
python -m aidars.scene_intelligence.cli aidars_scene_payload.json -o output/scene.json
```
