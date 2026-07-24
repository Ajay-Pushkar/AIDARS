"""Collection hierarchy extraction.

Executed inside Blender's embedded Python runtime (see inspect_scene.py).
Produces the ``collections`` block of the AIDARS scene payload, matching the
schema consumed by ``aidars.scene_intelligence.builders.CollectionBuilder``.
"""
from __future__ import annotations

import bpy


def _parent_collection_name(collection: "bpy.types.Collection") -> str | None:
    """Return the name of the first collection that has ``collection`` as a child.

    Blender collections don't store a direct back-reference to their parent,
    so the parent has to be found by scanning every other collection's
    ``children`` (this mirrors how the outliner determines nesting).
    """

    for candidate in bpy.data.collections:
        if candidate is collection:
            continue
        if collection.name in candidate.children.keys():
            return candidate.name

    # A collection linked directly under the scene's root collection (not
    # nested under another user collection) has no parent.
    if collection.name in bpy.context.scene.collection.children.keys():
        return None

    return None


def extract_collections() -> list:
    """Extract every collection in the current .blend file, with parent links.

    Returns:
        A list of dictionaries, each with ``name``, ``id``, and ``parent``
        keys. ``id`` mirrors ``name`` since collection names are unique
        within a .blend file.
    """

    collections = []
    for collection in bpy.data.collections:
        collections.append(
            {
                "name": collection.name,
                "id": collection.name,
                "parent": _parent_collection_name(collection),
            }
        )
    return collections
