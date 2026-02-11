APP_NAME ?= image-resize
VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
PIP ?= $(VENV)/bin/pip

.PHONY: build run test install

build:
	docker-compose build

run:
	docker-compose up

install: $(VENV)/bin/activate

$(VENV)/bin/activate:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

test: install
	PYTHONPATH=. $(PYTHON) -m pytest -q


