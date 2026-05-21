# Build Workflows That Think, Decide, and Act Across Systems

An agentic PR review pipeline that orchestrates GitHub, Snowflake Cortex AI, Slack, and human approvals — with durable execution and full observability.

## Quick Start

1. **Snowflake** — Run `setup.sql` in a Snowsight SQL Worksheet
2. **Python** — `uv pip install prefect requests pandas "snowflake-connector-python[pandas]"`
3. **Auth** — `prefect cloud login`
4. **Configure** — Set env vars (see below)
5. **Run** — `python pr_review_pipeline.py`

## Environment Variables

```bash
export GITHUB_TOKEN=your_github_pat
export SNOWFLAKE_CONNECTION_NAME=your_connection
export SLACK_BOT_TOKEN=xoxb-your-bot-token
export SLACK_CHANNEL=your-channel
export PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
```

## Snowflake Connection

Configure in `~/.snowflake/connections.toml`:

```toml
[your_connection]
account = "your-account"
user = "your-user"
authenticator = "externalbrowser"
role = "ACCOUNTADMIN"
warehouse = "PREFECT_WH"
database = "PREFECT_QUICKSTART"
schema = "RAW"
```

## Slack App Setup

1. Create a Slack App at https://api.slack.com/apps
2. Add Bot Token Scopes: `chat:write`
3. Install to workspace
4. Invite bot to channel: `/invite @YourApp`

## GitHub Token

PAT (classic) with `repo` scope.

## Files

- `pr_review_pipeline.py` — Complete pipeline
- `setup.sql` — Snowflake DDL + seed data
- `prefect.yaml` — Deployment config
- `guide/` — Quickstart guide (md + rendered HTML)

## Guide

Open `guide/getting-started-with-prefect-and-snowflake.html` for the rendered quickstart.
