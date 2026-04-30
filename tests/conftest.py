"""Shared pytest fixtures + sys.path bootstrap.

Project layout is flat (analysis_engine.py / piotroski.py at REPO root, not in
src/). Adding REPO to sys.path here so tests can `from piotroski import ...`
without packaging gymnastics.
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
