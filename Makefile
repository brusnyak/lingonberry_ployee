VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: install repl ask autoresearch

install:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

repl:
	$(PYTHON) brain.py

ask:
	@read -p "Ask: " q; $(PYTHON) -c "from brain import ask; print(ask('$q'))"

autoresearch:
	@if [ -z "$(PROGRAM)" ]; then echo "Usage: make autoresearch PROGRAM=programs/outreach_copy.md [MAX=5] [DRY=1]"; exit 1; fi
	$(PYTHON) autoresearch.py $(PROGRAM) \
		$(if $(MAX),--max $(MAX),) \
		$(if $(DRY),--dry-run,) \
		$(if $(PAUSE),--pause $(PAUSE),)
