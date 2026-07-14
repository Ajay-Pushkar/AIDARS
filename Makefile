PYTHON ?= python

.PHONY: test lint run-example

test:
	$(PYTHON) -m unittest discover -s tests -v

run-example:
	$(PYTHON) -m aidars.scene_intelligence.cli tests/fixtures/scene_payload.json --package --frame-start 1 --frame-end 24 --package-output output/package.json
