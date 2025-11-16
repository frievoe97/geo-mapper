PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

.PHONY: venv install run clean

venv:
	python3 -m venv .venv

install: venv
	$(PIP) install -r requirements.txt

run:
	$(PYTHON) clean_geojson_data.py

clean:
	rm -rf geodata_clean
