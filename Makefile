.PHONY: install lint format typecheck test check collect status normalize verify

install:
	./scripts/install.sh

lint:
	.venv/bin/ruff check .

format:
	.venv/bin/ruff format .

typecheck:
	.venv/bin/mypy src

test:
	.venv/bin/pytest -m "not live"

check: lint typecheck test

collect:
	.venv/bin/polymarket-bt collect --config configs/collector.yaml

status:
	.venv/bin/polymarket-bt status --config configs/collector.yaml

normalize:
	.venv/bin/polymarket-bt normalize --config configs/collector.yaml

verify:
	.venv/bin/polymarket-bt verify-files --config configs/collector.yaml
