"""
Formatting helpers for the Limnos MCP server.
"""

from __future__ import annotations

from typing import Any, Dict, List


def format_table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    """Format a list of dictionaries as a Markdown table."""
    if not rows:
        return "_No results found._"

    # Header
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"

    # Rows
    md_rows = []
    for row in rows:
        md_row = "| " + " | ".join(str(row.get(col, "")) for col in columns) + " |"
        md_rows.append(md_row)

    return "\n".join([header, separator] + md_rows)


def format_query_result(result: Any, summary: str) -> str:
    """Format a QueryResult object as a Markdown response with metadata."""
    table_md = format_table(result.rows, result.columns)

    response = (
        f"### Query Results\n\n"
        f"**{summary}**\n\n"
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Engine | {result.engine} |\n"
        f"| Rows | {result.row_count}{' (truncated)' if result.truncated else ''} |\n"
        f"| Duration | {result.duration_ms}ms |\n"
        f"| Bytes scanned | {result.bytes_scanned if result.bytes_scanned >= 0 else 'unknown'} |\n\n"
        f"{table_md}\n\n"
        f"**SQL executed:**\n```sql\n{result.sql_executed}\n```"
    )

    return response
