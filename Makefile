.PHONY: check
check:
	ruff format .
	ruff . --fix
	mypy
	pytest

.PHONY: serve
serve:
	textual run --dev -c harlequin -P None -f .