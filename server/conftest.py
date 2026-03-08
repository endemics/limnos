"""Pytest configuration — add server/ to sys.path so tests can import server modules."""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
