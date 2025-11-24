PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

.PHONY: venv install run clean

venv:
	python3 -m venv .venv

install: venv
	$(PIP) install -e .

run:
	$(PYTHON) -m geo_mapper --help

clean:
	rm -rf results
