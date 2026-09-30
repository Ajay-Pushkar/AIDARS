PYTHON ?= python

.PHONY: test test-q start-coordinator start-worker run-example lint

test:
	$(PYTHON) -m pytest tests/ -v

test-q:
	$(PYTHON) -m pytest tests/ -q

run-example:
	$(PYTHON) -m aidars.adapters.blender.intelligence.cli tests/fixtures/scene_payload.json --package --frame-start 1 --frame-end 24 --package-output output/package.json

# M10.13: run a coordinator/worker process locally. Every setting is
# environment-variable/CLI-flag driven -- see src/aidars/distributed/cli.py
# and docs/M10_ARCHITECTURE.md's "Configuration" section for the full list.
start-coordinator:
	$(PYTHON) -m aidars.distributed.cli coordinator

start-worker:
	$(PYTHON) -m aidars.distributed.cli worker
