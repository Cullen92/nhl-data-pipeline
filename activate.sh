#!/bin/bash
# Activate the Python virtual environment
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
else
  echo "Error: No virtual environment found. Expected .venv/ or venv/ directory." >&2
  exit 1
fi
echo "✓ Virtual environment activated"
echo "Python: $(which python)"
echo "To deactivate, run: deactivate"
