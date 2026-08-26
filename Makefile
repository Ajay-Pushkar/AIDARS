PYTHON ?= python

.PHONY: test test-q start-coordinator start-worker run-example lint

test:
	$(PYTHON) -m pytest tests/ -v

test-q:
	$(PYTHON) -m pytest tests/ -q

run-example:
	$(PYTHON) -m aidars.scene_intelligence.cli tests/fixtures/scene_payload.json --package --frame-start 1 --frame-end 24 --package-output output/package.json
