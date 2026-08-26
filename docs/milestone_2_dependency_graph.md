# Milestone 2: Hierarchical Dependency Graph & Topological Resolution

**Module:** `src/aidars/scene_intelligence/`  
**Status:** Completed & Validated  

---

## 1. Overview
Milestone 2 constructs a directed acyclic graph (DAG) representing the complete hierarchy and asset dependency structure of a 3D scene.

```text
Scene Root
    │
    ├── Object (Mesh / Camera / Light)
    │     │
    │     ├── Modifier / Simulation Cache
    │     │
    │     └── Material
    │           │
    │           └── Shader Node / Texture
    │                 │
    │                 └── Image / External Asset
```

---

## 2. Core Capabilities
- **Hierarchical Node Mapping**: Standardized node taxonomy (`SCENE`, `OBJECT`, `MESH`, `MATERIAL`, `TEXTURE`, `IMAGE`, `SIMULATION`).
- **Edge Relationship Classification**: Strongly typed edges (`PARENT_OF`, `MODIFIED_BY`, `USES_MATERIAL`, `USES_TEXTURE`, `REFERENCES_IMAGE`).
- **Cycle-Safe Traversal**: Breadth-first search (BFS) and topological sort algorithms with cycle detection.
- **Integrity & Orphan Detection**: Identifies unreferenced materials, dangling texture paths, and missing image files.

---

## 3. Key Components
- `scene_intelligence/graph_builder.py`: Constructs the full dependency graph from M1 canonical snapshots.
- `scene_intelligence/graph_models.py`: Strongly typed graph nodes, edges, and validation records.
- `scene_intelligence/integrity_checker.py`: Validates topological consistency and highlights missing links.
