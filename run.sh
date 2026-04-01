#!/usr/bin/env bash
# Run the agent REPL from anywhere inside the biz project.
# Usage: bash agent/run.sh   (from biz root)
#        ./run.sh            (from agent/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/brain.py" "$@"
