.PHONY: format
format:
	poetry run isort pybrook/ tests/
	poetry run yapf -i -vv -r pybrook/ tests/

.PHONY: lint
lint:
	-poetry run mypy --config-file pyproject.toml $(shell pwd)/pybrook
	-poetry run flake8 $(shell pwd)/pybrook

.PHONY: test
test:
	poetry run pytest --cov=./pybrook -vvv --cov-report html

.PHONY: build_docs
build_docs:
	poetry run mkdocs build

.PHONY: serve_docs
serve_docs:
	poetry run mkdocs serve

.PHONY: thesis
thesis:
	$(MAKE) -C $@
	evince thesis/thesis.pdf
