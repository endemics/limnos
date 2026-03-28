"""Tests for query helpers."""

from __future__ import annotations

from tools.query import current_query_cost


def test_current_query_cost_contextvar():
    current_query_cost.set(7.89)
    assert current_query_cost.get() == 7.89
    current_query_cost.set(0.0)
    assert current_query_cost.get() == 0.0
