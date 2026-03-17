VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: install repl ask

install:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

repl:
	$(PYTHON) brain.py

ask:
	@read -p "Ask: " q; $(PYTHON) -c "from brain import ask; print(ask('$$q'))"
