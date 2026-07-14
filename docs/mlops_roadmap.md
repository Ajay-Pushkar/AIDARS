# AIDARS MLOps and Project Structure Roadmap

This document translates the requested MLOps and production-readiness blueprint into a phased plan that fits the current AIDARS repository.

## Phase 1 — Foundation (Completed / in progress)

Goals:
- Establish a clean Python package structure under src/
- Keep scene analysis, packaging, and graph generation modular
- Add tests and basic CLI workflow

Current state:
- Core package under src/aidars/
- Scene intelligence engine, loader, scanner, exporter, dependency graph, and smart packaging builder are implemented
- Unit tests exist and pass

## Phase 2 — Reproducibility and Packaging

Recommended next steps:
- Add a pinned environment file (requirements.txt or uv/poetry config)
- Add a Makefile or scripts for local workflows
- Introduce config-driven settings for frame range, output paths, and adapter behavior
- Add a simple CI workflow for tests

Suggested files:
- requirements.txt
- Makefile
- .github/workflows/ci.yml

## Phase 3 — Data and Experiment Versioning

Recommended additions:
- Version scene fixtures and example payloads in a data/ or examples/ folder
- Track pipeline inputs/outputs with DVC or a lightweight local manifest system
- Add experiment tracking later if model-based workflows are introduced

Suggested folders:
- data/
- examples/
- experiments/

## Phase 4 — Testing and Quality Gates

Recommended additions:
- Add pytest for broader regression testing
- Add schema validation for scene payloads using Pydantic or JSON Schema
- Add integration tests for the CLI end to end
- Introduce linting and formatting checks

Suggested tools:
- pytest
- ruff
- mypy

## Phase 5 — Production Readiness

Once the core scene intelligence pipeline is stable:
- Add containerization with Docker
- Add API serving with FastAPI
- Add model/asset serving metadata and monitoring hooks
- Add secrets handling and safe config loading

Suggested tools:
- Docker
- FastAPI
- MLflow or similar tracking
- Prometheus/Grafana for monitoring

## Recommended Repository Shape

A practical structure for this repository would be:

```text
project/
├── data/                # Example scenes, fixture payloads, outputs
├── docs/                # Architecture and roadmap docs
├── notebooks/          # Optional exploratory analysis
├── src/aidars/          # Core package
├── tests/               # Unit and integration tests
├── scripts/             # Utility scripts
├── pyproject.toml       # Packaging and dependencies
├── Makefile             # Local workflows
└── README.md            # Quickstart and usage
```

## Practical Recommendation

Do not introduce the full MLOps stack immediately. Start with:
1. a simple reproducible environment,
2. config-driven CLI workflows,
3. test automation,
4. structured docs and examples,
5. then add containerization and serving later.
