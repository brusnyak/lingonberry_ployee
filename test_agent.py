"""
agent/test_agent.py
Interactive terminal test for the agent — shows full reasoning trace.
Usage:
  cd agent && python test_agent.py
  cd agent && python test_agent.py "run a dry-run engagement session"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import brain


def run(prompt: str) -> None:
    print(f"\n{'='*60}")
    print(f"PROMPT: {prompt}")
    print('='*60)

    def on_activity(line: str):
        print(line, flush=True)

    result = brain.ask(prompt, verbose=True, activity_cb=on_activity)
    print(f"\n{'='*60}")
    print("RESULT:")
    print(result)
    print('='*60)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(" ".join(sys.argv[1:]))
    else:
        print("Interactive agent test. Type your message, empty line to quit.")
        while True:
            try:
                prompt = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not prompt:
                break
            run(prompt)
