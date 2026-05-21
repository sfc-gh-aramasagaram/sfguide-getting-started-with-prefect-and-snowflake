import json
import os
import re
from datetime import datetime
import pandas as pd
import requests
import snowflake.connector
from prefect import flow, task, get_run_logger, pause_flow_run
from prefect.input import RunInput
from prefect.tasks import exponential_backoff
from snowflake.connector.pandas_tools import write_pandas

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
SF_CONNECTION = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "ARAMASAGARAM_AWS1")

_sf_conn = None

def get_connection():
    global _sf_conn
    if _sf_conn is None or _sf_conn.is_closed():
        _sf_conn = snowflake.connector.connect(connection_name=SF_CONNECTION)
        _sf_conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'prefect-pr-review'")
    return _sf_conn


@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def fetch_pr_data(repo: str, pr_number: int) -> dict:
    logger = get_run_logger()
    logger.info(f"Fetching PR #{pr_number} from {repo}...")

    headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

    pr_resp = requests.get(
        f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
        headers=headers,
        timeout=30,
    )
    pr_resp.raise_for_status()
    pr_data = pr_resp.json()

    diff_resp = requests.get(
        f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
        headers={**headers, "Accept": "application/vnd.github.v3.diff"},
        timeout=30,
    )
    diff_resp.raise_for_status()

    result = {
        "pr_number": pr_number,
        "repo": repo,
        "title": pr_data["title"],
        "author": pr_data["user"]["login"],
        "diff_content": diff_resp.text[:50000],
        "files_changed": pr_data["changed_files"],
        "additions": pr_data["additions"],
        "deletions": pr_data["deletions"],
        "created_at": pr_data["created_at"],
    }

    logger.info(
        f"Fetched: '{result['title']}' by {result['author']} "
        f"({result['files_changed']} files, +{result['additions']}/-{result['deletions']})"
    )
    return result


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def load_pr_to_snowflake(pr_data: dict) -> int:
    logger = get_run_logger()
    conn = get_connection()

    df = pd.DataFrame([{
        "PR_NUMBER": pr_data["pr_number"],
        "REPO": pr_data["repo"],
        "TITLE": pr_data["title"],
        "AUTHOR": pr_data["author"],
        "DIFF_CONTENT": pr_data["diff_content"],
        "FILES_CHANGED": pr_data["files_changed"],
        "ADDITIONS": pr_data["additions"],
        "DELETIONS": pr_data["deletions"],
        "CREATED_AT": pd.Timestamp(pr_data["created_at"]),
    }])

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="PULL_REQUESTS",
        database="PREFECT_QUICKSTART",
        schema="RAW",
    )

    logger.info(f"Loaded PR #{pr_data['pr_number']} to Snowflake ({num_rows} row)")
    return num_rows


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def cortex_assess_pr(pr_data: dict) -> dict:
    logger = get_run_logger()
    logger.info("Running Cortex AI assessment...")
    conn = get_connection()

    diff_preview = pr_data["diff_content"][:5000]

    pr_context = (
        f"PR Title: {pr_data['title']}\n"
        f"Files changed: {pr_data['files_changed']}, "
        f"+{pr_data['additions']}/-{pr_data['deletions']}\n"
        f"Diff:\n{diff_preview}"
    )

    summarize_prompt = (
        f"Summarize this pull request in 2-3 sentences for a technical reviewer. "
        f"Focus on what changed and potential impact.\n\n{pr_context}"
    )

    cur = conn.cursor()
    cur.execute(
        "SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(%s, ['low', 'medium', 'high'])",
        (pr_context,),
    )
    risk_level = cur.fetchone()[0].strip().lower()

    cur.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", ("mistral-large2", summarize_prompt))
    summary = cur.fetchone()[0].strip()
    cur.close()

    if risk_level not in ("low", "medium", "high"):
        risk_level = "medium"

    logger.info(f"Cortex assessment: risk={risk_level}")
    logger.info(f"Summary: {summary[:200]}...")

    return {"risk_level": risk_level, "summary": summary}


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def get_historical_context(pr_data: dict) -> dict:
    logger = get_run_logger()
    conn = get_connection()

    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS total_prs, "
        "SUM(CASE WHEN HAD_INCIDENT THEN 1 ELSE 0 END) AS incident_count, "
        "MAX(MERGED_AT) AS last_pr_date, "
        "LISTAGG(DISTINCT RISK_LEVEL, ', ') AS past_risk_levels "
        "FROM PREFECT_QUICKSTART.ANALYTICS.PR_HISTORY "
        "WHERE AUTHOR = %s",
        (pr_data["author"],),
    )
    result = cur.fetchone()
    cur.close()

    context = {
        "total_prs": result[0] or 0,
        "incident_count": result[1] or 0,
        "last_pr_date": str(result[2]) if result[2] else "N/A",
        "past_risk_levels": result[3] or "none",
    }

    logger.info(
        f"Historical context for {pr_data['author']}: "
        f"{context['total_prs']} PRs, {context['incident_count']} incidents"
    )
    return context


@task(log_prints=True)
def make_decision(pr_data: dict, assessment: dict, history: dict) -> dict:
    logger = get_run_logger()
    risk = assessment["risk_level"]

    if risk == "high" or (risk == "medium" and history["incident_count"] > 0):
        effective_risk = "high"
    elif risk == "medium":
        effective_risk = "medium"
    else:
        effective_risk = "low"

    logger.info(
        f"Decision: cortex_risk={risk}, effective_risk={effective_risk} "
        f"(author incidents: {history['incident_count']})"
    )

    return {
        "effective_risk": effective_risk,
        "requires_approval": effective_risk in ("medium", "high"),
        "auto_approved": effective_risk == "low",
    }


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def write_assessment(pr_data: dict, assessment: dict, decision: dict, reviewer: str = "auto", reviewer_notes: str = "") -> None:
    logger = get_run_logger()
    conn = get_connection()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO PREFECT_QUICKSTART.ANALYTICS.PR_ASSESSMENTS "
        "(PR_NUMBER, REPO, RISK_LEVEL, SUMMARY, DECISION, REVIEWER, REVIEWER_NOTES) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            pr_data["pr_number"],
            pr_data["repo"],
            decision["effective_risk"],
            assessment["summary"],
            "approved" if decision.get("final_approved", True) else "rejected",
            reviewer,
            reviewer_notes,
        ),
    )
    cur.close()

    logger.info(f"Assessment written to Snowflake for PR #{pr_data['pr_number']}")



@task(retries=1, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def slack_approval_request(pr_data: dict, assessment: dict, decision: dict, flow_run_id: str) -> None:
    logger = get_run_logger()
    slack_bot_token = os.environ.get("SLACK_BOT_TOKEN")
    slack_channel = os.environ.get("SLACK_CHANNEL", "#prefect-demo")

    risk_emoji = {"low": ":white_check_mark:", "medium": ":warning:", "high": ":rotating_light:"}
    emoji = risk_emoji.get(decision["effective_risk"], ":question:")

    flow_run_url = (
        f"https://app.prefect.cloud/account/56faea9b-5ee1-448b-b375-1a5a501c48a8"
        f"/workspace/53cd4738-566b-4442-90bc-cc458d8f63cf/runs/flow-run/{flow_run_id}"
    )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji} PR Review Required: #{pr_data['pr_number']}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Repo:*\n`{pr_data['repo']}`"},
                {"type": "mrkdwn", "text": f"*Risk Level:*\n{decision['effective_risk'].upper()}"},
                {"type": "mrkdwn", "text": f"*Author:*\n{pr_data['author']}"},
                {"type": "mrkdwn", "text": f"*Files Changed:*\n{pr_data['files_changed']}"},
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Title:* {pr_data['title']}\n\n*AI Summary:* {assessment['summary'][:500]}"}
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":rotating_light: *Action required — approve or reject in Prefect:*"},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":white_check_mark: Review & Approve"},
                    "url": flow_run_url,
                    "style": "primary",
                    "action_id": "open_prefect",
                },
            ]
        }
    ]

    if slack_bot_token:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {slack_bot_token}", "Content-Type": "application/json"},
            json={"channel": slack_channel, "blocks": blocks, "text": f"PR #{pr_data['pr_number']} needs review ({decision['effective_risk'].upper()} risk)"},
            timeout=10,
        )
        resp_data = response.json()
        if not resp_data.get("ok"):
            logger.warning(f"Slack error: {resp_data.get('error')}")
        else:
            logger.info(f"Slack approval request sent to {slack_channel}")
    else:
        logger.info(f"[Slack preview] PR #{pr_data['pr_number']} needs review — {flow_run_url}")


@task(retries=1, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def slack_completion_notice(pr_data: dict, assessment: dict, decision: dict) -> None:
    logger = get_run_logger()
    slack_bot_token = os.environ.get("SLACK_BOT_TOKEN")
    slack_channel = os.environ.get("SLACK_CHANNEL", "#prefect-demo")

    approved = decision.get("final_approved", False)
    status_emoji = ":white_check_mark:" if approved else ":x:"
    status_text = "APPROVED" if approved else "REJECTED"
    reviewer = decision.get("reviewer", "auto")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{status_emoji} PR #{pr_data['pr_number']} — {status_text}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Repo:*\n`{pr_data['repo']}`"},
                {"type": "mrkdwn", "text": f"*Risk Level:*\n{decision['effective_risk'].upper()}"},
                {"type": "mrkdwn", "text": f"*Reviewer:*\n{reviewer}"},
                {"type": "mrkdwn", "text": f"*Decision:*\n{status_text}"},
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Title:* {pr_data['title']}\n*Summary:* {assessment['summary'][:300]}"}
        },
    ]

    if slack_bot_token:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {slack_bot_token}", "Content-Type": "application/json"},
            json={"channel": slack_channel, "blocks": blocks, "text": f"PR #{pr_data['pr_number']} {status_text} by {reviewer}"},
            timeout=10,
        )
        resp_data = response.json()
        if not resp_data.get("ok"):
            logger.warning(f"Slack error: {resp_data.get('error')}")
        else:
            logger.info(f"Slack completion notice sent ({status_text})")
    else:
        logger.info(f"[Slack preview] PR #{pr_data['pr_number']} {status_text} by {reviewer}")


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def close_github_pr(pr_data: dict, assessment: dict, decision: dict) -> None:
    logger = get_run_logger()
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    repo = pr_data["repo"]
    pr_number = pr_data["pr_number"]
    approved = decision.get("final_approved", False)
    reviewer = decision.get("reviewer", "auto")
    risk = decision["effective_risk"].upper()

    status_emoji = "\u2705" if approved else "\u274c"
    comment_body = (
        f"## {status_emoji} Prefect PR Review — {'APPROVED' if approved else 'REJECTED'}\n\n"
        f"**Risk Level:** {risk}\n"
        f"**Reviewer:** {reviewer}\n\n"
        f"### Cortex AI Summary\n"
        f"{assessment['summary']}\n\n"
        f"---\n"
        f"*Assessed by [Prefect + Snowflake Cortex AI](https://prefect.io) pipeline*"
    )

    r = requests.post(
        f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments",
        headers=headers,
        json={"body": comment_body},
        timeout=15,
    )
    r.raise_for_status()
    logger.info(f"Posted review comment on PR #{pr_number}")

    if approved:
        r = requests.put(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
            headers=headers,
            json={"commit_title": f"Merge PR #{pr_number}: {pr_data['title']}", "merge_method": "squash"},
            timeout=15,
        )
        if r.status_code == 200:
            logger.info(f"PR #{pr_number} merged successfully")
        else:
            logger.warning(f"Could not merge PR #{pr_number}: {r.status_code} {r.json().get('message', '')}")
    else:
        r = requests.patch(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
            headers=headers,
            json={"state": "closed"},
            timeout=15,
        )
        r.raise_for_status()
        logger.info(f"PR #{pr_number} closed (rejected)")


@task(retries=1, retry_delay_seconds=exponential_backoff(backoff_factor=2), log_prints=True)
def export_report(pr_data: dict, assessment: dict, decision: dict, history: dict) -> str:
    logger = get_run_logger()

    report = {
        "pr_number": pr_data["pr_number"],
        "repo": pr_data["repo"],
        "title": pr_data["title"],
        "author": pr_data["author"],
        "risk_level": decision["effective_risk"],
        "summary": assessment["summary"],
        "historical_context": history,
        "timestamp": datetime.now().isoformat(),
    }

    os.makedirs("./reports", exist_ok=True)
    filepath = f"./reports/pr_{pr_data['pr_number']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filepath, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Report exported to {filepath}")
    return filepath


class ReviewDecision(RunInput):
    approved: bool
    reviewer_name: str
    notes: str = ""


@flow(name="pr-review-agent", log_prints=True)
def pr_review_agent(repo: str, pr_number: int):
    logger = get_run_logger()

    from prefect.runtime import flow_run as flow_run_ctx
    flow_run_id = str(flow_run_ctx.id)

    get_connection()
    logger.info("Snowflake connection established")

    try:
        pr_data = fetch_pr_data(repo=repo, pr_number=pr_number)
        load_pr_to_snowflake(pr_data)
        assessment = cortex_assess_pr(pr_data)
        history = get_historical_context(pr_data)
        decision = make_decision(pr_data, assessment, history)

        if decision["requires_approval"]:
            logger.info(
                f"Risk is {decision['effective_risk'].upper()} — "
                f"pausing for human review..."
            )

            slack_approval_request(pr_data, assessment, decision, flow_run_id)

            review = pause_flow_run(
                wait_for_input=ReviewDecision.with_initial_data(
                    approved=False,
                    reviewer_name="",
                ),
                timeout=86400,
            )

            decision["final_approved"] = review.approved
            decision["auto_approved"] = False
            decision["reviewer"] = review.reviewer_name

            write_assessment(
                pr_data, assessment, decision,
                reviewer=review.reviewer_name,
                reviewer_notes=review.notes,
            )

            if not review.approved:
                logger.warning(f"PR #{pr_number} REJECTED by {review.reviewer_name}")
                close_github_pr(pr_data, assessment, decision)
                slack_completion_notice(pr_data, assessment, decision)
                return

            logger.info(f"PR #{pr_number} APPROVED by {review.reviewer_name}")

        else:
            logger.info(f"Risk is LOW — auto-approving PR #{pr_number}")
            decision["final_approved"] = True
            decision["reviewer"] = "auto"
            write_assessment(pr_data, assessment, decision)

        close_github_pr(pr_data, assessment, decision)
        slack_completion_notice(pr_data, assessment, decision)
        report_path = export_report(pr_data, assessment, decision, history)

        logger.info(f"Pipeline complete. Report: {report_path}")

    finally:
        global _sf_conn
        if _sf_conn and not _sf_conn.is_closed():
            _sf_conn.close()
            _sf_conn = None
        logger.info("Snowflake connection closed")


if __name__ == "__main__":
    pr_review_agent(repo="snowflake-corp/prefect-snowflake-demo", pr_number=4)
