"""
MDBP GitHub Events — Live MCP Server

Connects to ClickHouse Playground's github_events table (10.6B rows of real GitHub data)
and exposes it as an MCP server. No API key, no signup, no setup.

Usage:
    pip install mdbp clickhouse-sqlalchemy clickhouse-driver
    python server.py

Then connect from Claude Desktop, Cursor, or any MCP client.

Example queries the LLM can make:
    - "Show me the most starred repos today"
    - "How many pull requests were opened this week?"
    - "Top 10 contributors to the Python language"
    - "Compare push events: JavaScript vs Rust this month"
"""

from mdbp import MDBP
from mdbp.core.policy import Policy
from mdbp.core.schema_registry import EntitySchema, FieldSchema
from mdbp.core.audit import StreamAuditLogger
from mdbp.transport.server import create_server, run_sse, run_stdio

DB_URL = "clickhouse+native://explorer:@play.clickhouse.com:9440/default?secure=true"

# ── MDBP Setup ──────────────────────────────────────────────

mdbp = MDBP(
    db_url=DB_URL,
    auto_discover=False,  # ClickHouse Enum types break auto-discover on Python 3.14
    allowed_intents=["list", "get", "count", "aggregate"],  # read-only
    audit=StreamAuditLogger(),
)

# ── Schema Registration ─────────────────────────────────────

mdbp.register_entity(EntitySchema(
    entity="github_event",
    table="github_events",
    primary_key="file_time",
    description="Real-time GitHub events: pushes, PRs, issues, stars, forks, comments. 10.6 billion rows from 2011 to now.",
    fields={
        "file_time": FieldSchema(column="file_time", dtype="datetime", description="Event file timestamp"),
        "event_type": FieldSchema(column="event_type", dtype="text", description="Event type: PushEvent, WatchEvent, PullRequestEvent, IssuesEvent, ForkEvent, CreateEvent, etc."),
        "actor_login": FieldSchema(column="actor_login", dtype="text", description="GitHub username who triggered the event"),
        "repo_name": FieldSchema(column="repo_name", dtype="text", description="Full repository name (owner/repo)"),
        "created_at": FieldSchema(column="created_at", dtype="datetime", description="Event creation timestamp"),
        "action": FieldSchema(column="action", dtype="text", description="Action: opened, closed, created, merged, started, etc."),
        "title": FieldSchema(column="title", dtype="text", description="Issue/PR title"),
        "body": FieldSchema(column="body", dtype="text", description="Comment or issue body text"),
        "state": FieldSchema(column="state", dtype="text", description="State: open, closed, none"),
        "number": FieldSchema(column="number", dtype="integer", description="Issue/PR number"),
        "comments": FieldSchema(column="comments", dtype="integer", description="Comment count on issue/PR"),
        "commits": FieldSchema(column="commits", dtype="integer", description="Commit count in PR"),
        "additions": FieldSchema(column="additions", dtype="integer", description="Lines added in PR"),
        "deletions": FieldSchema(column="deletions", dtype="integer", description="Lines deleted in PR"),
        "changed_files": FieldSchema(column="changed_files", dtype="integer", description="Files changed in PR"),
        "push_size": FieldSchema(column="push_size", dtype="integer", description="Number of commits in push"),
        "merged": FieldSchema(column="merged", dtype="integer", description="Whether PR was merged (1=yes, 0=no)"),
        "ref": FieldSchema(column="ref", dtype="text", description="Branch reference"),
        "head_ref": FieldSchema(column="head_ref", dtype="text", description="PR head branch"),
        "base_ref": FieldSchema(column="base_ref", dtype="text", description="PR base branch"),
        "member_login": FieldSchema(column="member_login", dtype="text", description="Added member username"),
        "release_tag_name": FieldSchema(column="release_tag_name", dtype="text", description="Release tag name"),
        "release_name": FieldSchema(column="release_name", dtype="text", description="Release name"),
        "labels": FieldSchema(column="labels", dtype="text", description="Issue/PR labels", filterable=False, sortable=False),
    },
))

# ── Policies ─────────────────────────────────────────────────

# Default: read-only, max 100 rows per query
mdbp.add_policy(Policy(
    entity="github_event",
    role="*",
    max_rows=100,
    allowed_intents=["list", "count", "aggregate"],
))

# Analyst: more rows
mdbp.add_policy(Policy(
    entity="github_event",
    role="analyst",
    max_rows=1000,
    allowed_intents=["list", "count", "aggregate"],
))


# ── Run ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"

    if transport == "sse":
        print("Starting GitHub Events MCP server on http://127.0.0.1:8000/sse")
        run_sse(mdbp, host="127.0.0.1", port=8000)
    else:
        run_stdio(mdbp)
