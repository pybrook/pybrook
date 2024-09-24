.PHONY: format
format:
	pdm run ruff format pybrook/ tests/

.PHONY: lint
lint:
	-pdm run mypy --config-file pyproject.toml $(shell pwd)/pybrook
	-pdm run ruff check $(shell pwd)/pybrook

.PHONY: test
test:
	pdm run pytest --cov=./pybrook -vvv --cov-report html

.PHONY: build_docs
build_docs:
	pdm run mkdocs build

.PHONY: serve_docs
serve_docs:
	pdm run mkdocs serve
