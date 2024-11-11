all: setup-git-hooks install check test 

check: check-format check-lint

check-format:
	uv run ruff format . --diff

check-lint:
	uv run ruff check .

install:
	uv lock --locked
	uv sync --locked --group dev --group lint --group test

lint:
	uv run ruff format .
	uv run ruff check . --fix

setup-git-hooks:
	chmod +x hooks/pre-commit
	chmod +x hooks/pre-push
	git config core.hooksPath hooks

test:
	uv run pytest -v --cov=etl --cov-report=xml

.PHONY: all check check-format check-lint check-types install lint setup-git-hooks test