from __future__ import annotations

import json
import os
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape
from typing import Any, Dict
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.resend_client import ResendClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.services.acquisition_supervisor import (
    acquisition_digest,
    enrich_unsent_prospects,
    import_from_apollo_people_search,
    import_from_apollo_search,
)
from app.services.custom_outreach import outreach_status, run_custom_outreach_cycle
from app.services.post_purchase_autopilot import (
    run_paid_intake_reminder_sweep,
    run_post_delivery_upsell_sweep,
)
from app.services.relay_performance import (
    active_relay_query_hint,
    maybe_run_weekly_performance_review,
    relay_performance_status,
)
from app.services.relay_success_controller import (
    relay_success_status,
    run_relay_success_control_tick,
)


@dataclass
class OpsCycleResult:
    query: str
    search_result: Dict[str, Any]
    enrich_count: int
    outreach_result: Dict[str, Any]
    reminders_result: Dict[str, Any]
    upsell_result: Dict[str, Any]
    outreach_digest: Dict[str, Any]
    alerts_sent: list[str]
    errors: list[str]
    started_at: str
    finished_at: str


def _session() -> Session:
    return SessionLocal()


def _queries() -> list[str]:
    raw = os.getenv(
        "ACQ_OPS_QUERY_ROTATION",
        "ppc agency founder|google ads agency owner|paid media agency founder|performance marketing agency owner|google ads managing partner|media buying agency founder",
    )
    queries = [x.strip() for x in raw.split("|") if x.strip()]
    return queries or ["ppc agency founder"]


def choose_query(now: datetime | None = None) -> str:
    now = now or datetime.utcnow()
    try:
        query_hint = active_relay_query_hint(now)
        if query_hint:
            return query_hint
    except Exception:
        pass

    queries = _queries()
    idx = (now.timetuple().tm_yday + now.hour) % len(queries)
    return queries[idx]


def _alert_recipient() -> str:
    return os.getenv("OPS_ALERT_EMAIL", "").strip() or settings.reply_to_email or ""


def _send_html_email(subject: str, html: str) -> dict[str, Any]:
    to_email = _alert_recipient()
    if not to_email or not settings.resend_api_key:
        return {"status": "skipped", "reason": "missing_ops_alert_email_or_resend"}

    client = ResendClient()
    result = client.send_email(
        to_email=to_email,
        subject=subject,
        html=html,
        from_email=settings.from_email_fulfillment or settings.from_email_outbound,
        reply_to=settings.reply_to_email,
    )
    return {"status": "sent", "result": result}


def _seed_sender_address() -> str:
    return os.getenv("COLD_SMTP_MAILBOX_1_ADDRESS", "").strip()


def _seed_sender_password() -> str:
    return os.getenv("COLD_SMTP_MAILBOX_1_PASSWORD", "").strip()


def _seed_subject() -> str:
    return f"[AO Seed Check] {datetime.utcnow().strftime('%Y-%m-%d')}"


def _send_smtp_email_from_seed_sender(
    *,
    subject: str,
    text_body: str,
    html_body: str | None = None,
) -> dict[str, Any]:
    to_email = _alert_recipient()
    sender = _seed_sender_address()
    password = _seed_sender_password()

    if not to_email or not sender or not password:
        return {"status": "skipped", "reason": "missing_seed_sender_or_recipient"}

    host = os.getenv("COLD_SMTP_HOST", "smtp.porkbun.com").strip()
    port = int(os.getenv("COLD_SMTP_PORT", "587").strip() or "587")
    security = os.getenv("COLD_SMTP_SECURITY", "starttls").strip().lower()

    message = EmailMessage()
    message["From"] = sender
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")

    if security == "ssl":
        server = smtplib.SMTP_SSL(host, port, timeout=30)
    else:
        server = smtplib.SMTP(host, port, timeout=30)

    try:
        server.ehlo()
        if security == "starttls":
            server.starttls()
            server.ehlo()
        server.login(sender, password)
        server.sendmail(sender, [to_email], message.as_string())
    finally:
        try:
            server.quit()
        except Exception:
            pass

    return {
        "status": "sent",
        "sender": sender,
        "recipient": to_email,
        "subject": subject,
        "smtp_host": host,
        "smtp_port": port,
        "smtp_security": security,
    }


def _send_seed_check_email() -> dict[str, Any]:
    return _send_smtp_email_from_seed_sender(
        subject=_seed_subject(),
        text_body="quick daily seed check",
    )


def _chatgpt_check_block(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    email_lines = _email_activity_lines(outreach_digest, limit=3)
    return (
        f"AO seed subject: {seed_result.get('subject', '[AO Seed Check]')}\n"
        f"AO seed sender: {seed_result.get('sender', _seed_sender_address())}\n"
        "Seed landed in Gmail: [inbox / promotions / spam]\n\n"
        f"sent_today: {outreach_digest.get('sent_today', 0)}\n"
        f"replies_today: {outreach_digest.get('replies_today', 0)}\n"
        f"due_now_count: {outreach_digest.get('due_now_count', 0)}\n"
        f"in_sequence_count: {outreach_digest.get('in_sequence_count', 0)}\n"
        f"sender_counts_today: {json.dumps(outreach_digest.get('sender_counts_today', {}), ensure_ascii=False)}\n"
        f"reply_to_email: {outreach_digest.get('reply_to_email', '')}\n"
        f"reply_mailbox_address: {outreach_digest.get('reply_mailbox_address', '')}\n\n"
        f"money_today_usd: {today.get('gross_usd', 0)}\n"
        f"payments_today: {today.get('payments_count', 0)}\n"
        f"money_week_usd: {week.get('gross_usd', 0)}\n"
        f"payments_week: {week.get('payments_count', 0)}\n"
        f"money_month_usd: {month.get('gross_usd', 0)}\n"
        f"payments_month: {month.get('payments_count', 0)}\n\n"
        + "\n".join(email_lines)
        + "\n\nBased on this, what is the next highest-leverage legal, non-scammy move to increase replies or revenue without changing too many things at once?"
    )


def _latest_ops_state(session: Session) -> dict[str, Any] | None:
    stmt = (
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "ops_cycle_state")
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()
    if row is None or not row.payload_json:
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        return None


def _log_ops_state(session: Session, payload: dict[str, Any]) -> None:
    session.add(
        AcquisitionEvent(
            event_type="ops_cycle_state",
            prospect_external_id="ops",
            summary="custom outreach ops cycle completed",
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
    )


def _important_changes(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    changes: list[str] = []

    prev_outreach = (previous or {}).get("outreach_digest", {})
    curr_outreach = current.get("outreach_digest", {})

    for key in ["queued_count", "sent_today", "replies_today"]:
        prev = int(prev_outreach.get(key, 0))
        curr = int(curr_outreach.get(key, 0))
        if curr > prev:
            changes.append(f"{key} increased: {prev} -> {curr}")

    if current.get("errors"):
        changes.append("cycle_errors")

    sent_now = int(current.get("outreach_result", {}).get("send_result", {}).get("sent_count", 0))
    if sent_now > 0:
        changes.append(f"sent_now={sent_now}")

    auto_replied = int(current.get("outreach_result", {}).get("reply_result", {}).get("auto_replied", 0))
    if auto_replied > 0:
        changes.append(f"auto_replied={auto_replied}")

    reminder_sent = int(current.get("reminders_result", {}).get("sent_count", 0))
    if reminder_sent > 0:
        changes.append(f"intake_reminders={reminder_sent}")

    upsell_sent = int(current.get("upsell_result", {}).get("sent_count", 0))
    if upsell_sent > 0:
        changes.append(f"upsells={upsell_sent}")

    return changes


def _render_ops_alert_html(result: dict[str, Any], reasons: list[str]) -> str:
    query = result.get("query", "")
    search_result = result.get("search_result", {})
    outreach_result = result.get("outreach_result", {})
    outreach_digest = result.get("outreach_digest", {})

    lines = [
        "<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;color:#1f1f1f;line-height:1.6'>",
        f"<p><strong>Reasons:</strong> {escape(', '.join(reasons))}</p>",
        f"<p><strong>Query:</strong> {escape(query)}</p>",
        f"<p><strong>Search:</strong> searched={search_result.get('searched', 0)} upserted={search_result.get('upserted', 0)}</p>",
        f"<p><strong>Custom send result:</strong> {escape(json.dumps(outreach_result.get('send_result', {}), ensure_ascii=False))}</p>",
        f"<p><strong>Reply poll result:</strong> {escape(json.dumps(outreach_result.get('reply_result', {}), ensure_ascii=False))}</p>",
        f"<p><strong>Outreach digest:</strong> {escape(json.dumps(outreach_digest, ensure_ascii=False))}</p>",
        f"<p><strong>Errors:</strong> {escape(json.dumps(result.get('errors', []), ensure_ascii=False))}</p>",
        "</div>",
    ]
    return "\n".join(lines)


async def run_autonomous_cycle(
    *,
    force_query: str | None = None,
    send_live: bool | None = None,
    notify: bool = True,
) -> dict[str, Any]:
    started_at = datetime.utcnow().isoformat()
    query = force_query or choose_query()
    effective_send_live = settings.acq_auto_send if send_live is None else bool(send_live)
    errors: list[str] = []

    try:
        if settings.apollo_api_key and os.getenv("ACQ_OPS_SOURCE", "apollo_people").strip() == "apollo_people":
            search_result = await import_from_apollo_people_search({"q_keywords": query})
        else:
            search_result = await import_from_apollo_search({"q_keywords": query})
    except Exception as exc:
        search_result = {"status": "error", "searched": 0, "upserted": 0, "error": str(exc)}
        errors.append(f"search_error: {exc}")

    try:
        enrich_count = await enrich_unsent_prospects(limit=25)
    except Exception as exc:
        enrich_count = 0
        errors.append(f"enrich_error: {exc}")

    try:
        if effective_send_live:
            outreach_result = run_custom_outreach_cycle()
        else:
            outreach_result = {
                "status": "skipped",
                "summary": "send_live_false",
                "send_live": False,
                "snapshot": outreach_status(),
            }
    except Exception as exc:
        outreach_result = {"status": "error", "summary": str(exc)}
        errors.append(f"custom_outreach_error: {exc}")

    try:
        reminders_result = run_paid_intake_reminder_sweep(
            hours=int(os.getenv("OPS_INTAKE_REMINDER_HOURS", "12"))
        )
    except Exception as exc:
        reminders_result = {"status": "error", "sent_count": 0, "error": str(exc)}
        errors.append(f"reminder_error: {exc}")

    try:
        upsell_result = run_post_delivery_upsell_sweep(
            hours=int(os.getenv("OPS_UPSELL_DELAY_HOURS", "24"))
        )
    except Exception as exc:
        upsell_result = {"status": "error", "sent_count": 0, "error": str(exc)}
        errors.append(f"upsell_error: {exc}")

    try:
        success_control = run_relay_success_control_tick()
    except Exception as exc:
        success_control = {"status": "error", "summary": str(exc)}
        errors.append(f"success_control_error: {exc}")

    outreach_digest = outreach_status()

    try:
        performance_review = maybe_run_weekly_performance_review()
    except Exception as exc:
        performance_review = {"status": "error", "summary": str(exc)}
        errors.append(f"performance_review_error: {exc}")

    current_state = {
        "query": query,
        "search_result": search_result,
        "enrich_count": enrich_count,
        "outreach_result": outreach_result,
        "reminders_result": reminders_result,
        "upsell_result": upsell_result,
        "success_control": success_control,
        "outreach_digest": outreach_digest,
        "performance_review": performance_review,
        "errors": errors,
        "send_live": effective_send_live,
        "started_at": started_at,
        "finished_at": datetime.utcnow().isoformat(),
    }

    alerts_sent: list[str] = []

    with _session() as session:
        previous_state = _latest_ops_state(session)
        _log_ops_state(session, current_state)
        session.commit()

    reasons = _important_changes(previous_state, current_state)

    if notify and reasons:
        subject = f"[AO Ops] {', '.join(reasons[:3])}"
        html = _render_ops_alert_html(current_state, reasons)
        send_result = _send_html_email(subject, html)
        if send_result.get("status") == "sent":
            alerts_sent = reasons

    return {
        "query": query,
        "search_result": search_result,
        "enrich_count": enrich_count,
        "outreach_result": outreach_result,
        "reminders_result": reminders_result,
        "upsell_result": upsell_result,
        "success_control": success_control,
        "outreach_digest": outreach_digest,
        "performance_review": performance_review,
        "alerts_sent": alerts_sent,
        "errors": errors,
        "send_live": effective_send_live,
        "started_at": started_at,
        "finished_at": current_state["finished_at"],
    }


def ops_status() -> dict[str, Any]:
    with _session() as session:
        latest = _latest_ops_state(session)

    return {
        "latest_cycle": latest,
        "outreach_digest": outreach_status(),
        "relay_performance": relay_performance_status(),
        "relay_success": relay_success_status(),
        "query_rotation": _queries(),
    }



def _money_summary_window(days: int) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(days=days)

    with _session() as session:
        events = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "stripe_paid")
            .where(AcquisitionEvent.created_at >= cutoff)
            .order_by(AcquisitionEvent.created_at.desc())
        ).scalars().all()

        status_counts = acquisition_digest().get("status_counts", {})
        waiting_on_intake = int(status_counts.get("paid", 0))
        intake_received = int(status_counts.get("intake_received", 0))
        interested = int(status_counts.get("interested", 0))

    gross_cents = 0
    sales = {"one_packet": 0, "five_pack": 0, "monthly": 0, "unknown": 0}
    filtered_events = []

    for event in events:
        payload = {}
        try:
            payload = json.loads(event.payload_json or "{}")
        except Exception:
            payload = {}

        event_email = str(
            payload.get("customer_details", {}).get("email")
            or payload.get("customer_email")
            or payload.get("email")
            or ""
        ).strip().lower()

        if event_email == "pham.alann@gmail.com":
            continue

        filtered_events.append(event)

        amount_total = int(payload.get("amount_total") or 0)
        gross_cents += amount_total

        if amount_total == 4000:
            sales["one_packet"] += 1
        elif amount_total == 15000:
            sales["five_pack"] += 1
        elif amount_total == 75000:
            sales["monthly"] += 1
        else:
            sales["unknown"] += 1

    return {
        "days": days,
        "payments_count": len(filtered_events),
        "gross_usd": round(gross_cents / 100.0, 2),
        "gross_cents": gross_cents,
        "sales": sales,
        "buyers_waiting_on_intake": waiting_on_intake,
        "intake_received": intake_received,
        "interested": interested,
    }



def money_summary() -> dict[str, Any]:
    return {
        "today": _money_summary_window(1),
        "week": _money_summary_window(7),
        "month": _money_summary_window(30),
    }


def _daily_series_tz() -> ZoneInfo:
    tz_name = os.getenv("COLD_SEND_TIMEZONE", "America/New_York").strip() or "America/New_York"
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("America/New_York")


def _event_local_day(value: datetime | None, tz: ZoneInfo) -> date | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(tz).date()


def _event_utc_iso(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_event_payload(event: AcquisitionEvent) -> dict[str, Any]:
    try:
        payload = json.loads(event.payload_json or "{}")
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _stripe_event_email(payload: dict[str, Any]) -> str:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    return str(
        payload.get("customer_details", {}).get("email")
        or payload.get("customer_email")
        or payload.get("email")
        or raw_object.get("customer_details", {}).get("email")
        or raw_object.get("customer_email")
        or ""
    ).strip().lower()


def _stripe_event_amount_cents(payload: dict[str, Any]) -> int:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    try:
        return int(payload.get("amount_total") or raw_object.get("amount_total") or 0)
    except Exception:
        return 0


def _blank_daily_series_row(day: date) -> dict[str, Any]:
    return {
        "date": day.isoformat(),
        "gross_cents": 0,
        "money_usd": 0.0,
        "payments": 0,
        "sends": 0,
        "replies": 0,
    }


def daily_series(days: int | None = None) -> dict[str, Any]:
    tz = _daily_series_tz()
    today = datetime.now(tz).date()
    max_days = None
    if days is not None:
        max_days = max(1, min(int(days), 366))

    with _session() as session:
        events = list(
            session.execute(
                select(AcquisitionEvent)
                .where(
                    (AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
                    | (
                        AcquisitionEvent.event_type.in_(
                            ["custom_outreach_reply_seen", "smartlead_reply", "stripe_paid"]
                        )
                    )
                )
                .order_by(AcquisitionEvent.created_at.asc())
            ).scalars().all()
        )

    first_send = next(
        (
            event.created_at
            for event in events
            if str(event.event_type or "").startswith("custom_outreach_sent_step_")
        ),
        None,
    )
    first_event_day = min(
        (day for day in (_event_local_day(event.created_at, tz) for event in events) if day is not None),
        default=today,
    )
    start_day = _event_local_day(first_send, tz) or first_event_day
    if max_days is not None and (today - start_day).days + 1 > max_days:
        start_day = today - timedelta(days=max_days - 1)

    row_count = max((today - start_day).days + 1, 1)
    rows_by_day = {
        (start_day + timedelta(days=offset)).isoformat(): _blank_daily_series_row(start_day + timedelta(days=offset))
        for offset in range(row_count)
    }

    for event in events:
        local_day = _event_local_day(event.created_at, tz)
        if local_day is None:
            continue
        key = local_day.isoformat()
        row = rows_by_day.get(key)
        if row is None:
            continue

        event_type = str(event.event_type or "")
        if event_type.startswith("custom_outreach_sent_step_"):
            row["sends"] += 1
        elif event_type in {"custom_outreach_reply_seen", "smartlead_reply"}:
            row["replies"] += 1
        elif event_type == "stripe_paid":
            payload = _safe_event_payload(event)
            if _stripe_event_email(payload) == "pham.alann@gmail.com":
                continue
            amount_cents = _stripe_event_amount_cents(payload)
            row["payments"] += 1
            row["gross_cents"] += amount_cents
            row["money_usd"] = round(row["gross_cents"] / 100.0, 2)

    rows = list(rows_by_day.values())
    totals = {
        "gross_cents": sum(int(row["gross_cents"]) for row in rows),
        "payments": sum(int(row["payments"]) for row in rows),
        "sends": sum(int(row["sends"]) for row in rows),
        "replies": sum(int(row["replies"]) for row in rows),
    }
    totals["money_usd"] = round(totals["gross_cents"] / 100.0, 2)

    return {
        "timezone": str(tz),
        "start_date": start_day.isoformat(),
        "end_date": today.isoformat(),
        "first_send_at": _event_utc_iso(first_send),
        "totals": totals,
        "days": rows,
    }


def _format_sender_counts(sender_counts: dict[str, Any]) -> str:
    if not sender_counts:
        return "none yet"
    parts: list[str] = []
    for key, value in sender_counts.items():
        parts.append(f"{key}: {value}")
    return ", ".join(parts)



def _format_activity_email(entry: dict[str, Any], *, incoming: bool = False) -> str:
    company = str(entry.get("company_name") or "").strip()
    subject = str(entry.get("subject") or "(no subject)").strip()
    preview = str(entry.get("body_preview") or "").strip()
    name = str(entry.get("from_name") or entry.get("contact_name") or "").strip()
    address = str((entry.get("from_email") if incoming else entry.get("to_email")) or "").strip()
    sender = str(entry.get("sender_address") or "").strip()
    when = str(entry.get("created_at") or "").strip()
    parts: list[str] = []
    header = company or address or subject
    parts.append(header)
    if name and address and name.lower() != address.lower():
        parts.append(f"{name} <{address}>")
    elif address:
        parts.append(address)
    parts.append(f"subject: {subject}")
    if sender and not incoming:
        parts.append(f"sender: {sender}")
    if when:
        parts.append(f"at: {when}")
    if preview:
        parts.append(f"preview: {preview}")
    return " | ".join(parts)


def _email_activity_lines(outreach_digest: dict[str, Any], limit: int = 5) -> list[str]:
    sent = outreach_digest.get("recent_sent", [])[:limit]
    replies = outreach_digest.get("recent_replies", [])[:limit]
    auto_replies = outreach_digest.get("recent_auto_replies", [])[:limit]
    lines: list[str] = []

    reply_to_email = str(outreach_digest.get("reply_to_email") or "").strip()
    reply_mailbox = str(outreach_digest.get("reply_mailbox_address") or "").strip()
    if reply_to_email or reply_mailbox:
        lines.append(
            f"Reply routing: reply-to {reply_to_email or 'missing'} | IMAP mailbox {reply_mailbox or 'missing'}"
        )

    if sent:
        lines.append("Recent sent emails:")
        lines.extend(f"- {_format_activity_email(item)}" for item in sent)
    else:
        lines.append("Recent sent emails: none logged yet.")

    if replies:
        lines.append("Recent replies:")
        lines.extend(f"- {_format_activity_email(item, incoming=True)}" for item in replies)
    else:
        lines.append("Recent replies: none logged yet.")

    if auto_replies:
        lines.append("Recent auto replies:")
        lines.extend(f"- {_format_activity_email(item)}" for item in auto_replies)

    return lines


def _email_activity_html(outreach_digest: dict[str, Any], limit: int = 5) -> str:
    sent = outreach_digest.get("recent_sent", [])[:limit]
    replies = outreach_digest.get("recent_replies", [])[:limit]
    auto_replies = outreach_digest.get("recent_auto_replies", [])[:limit]
    reply_to_email = str(outreach_digest.get("reply_to_email") or "").strip() or "missing"
    reply_mailbox = str(outreach_digest.get("reply_mailbox_address") or "").strip() or "missing"

    def render_list(items: list[dict[str, Any]], incoming: bool = False) -> str:
        if not items:
            return "<p>none yet</p>"
        li = "".join(
            f"<li>{escape(_format_activity_email(item, incoming=incoming))}</li>" for item in items
        )
        return f"<ul>{li}</ul>"

    return (
        "<h3>Email routing</h3>"
        f"<p><strong>Reply-To:</strong> {escape(reply_to_email)}</p>"
        f"<p><strong>IMAP reply mailbox:</strong> {escape(reply_mailbox)}</p>"
        "<h3>Recent sent emails</h3>"
        f"{render_list(sent)}"
        "<h3>Recent replies</h3>"
        f"{render_list(replies, incoming=True)}"
        + (
            "<h3>Recent auto replies</h3>" + render_list(auto_replies)
            if auto_replies
            else ""
        )
    )


def _compact_sender_counts(sender_counts: dict[str, Any]) -> str:
    if not sender_counts:
        return "none yet"
    parts: list[str] = []
    for key, value in sender_counts.items():
        local = str(key).split("@", 1)[0]
        parts.append(f"{local} {value}")
    return " ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· ".join(parts)


def _truncate_text(value: str, limit: int = 96) -> str:
    value = " ".join(str(value or "").split())
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦"


def _compact_activity_line(entry: dict[str, Any], *, incoming: bool = False) -> str:
    company = str(entry.get("company_name") or "").strip()
    subject = str(entry.get("subject") or "").strip()
    preview = str(entry.get("body_preview") or "").strip()
    sender = str(entry.get("sender_address") or "").strip()
    address = str((entry.get("from_email") if incoming else entry.get("to_email")) or "").strip()

    who = company or address or "(unknown)"
    parts: list[str] = [who]

    if sender and not incoming:
        parts.append(sender.split("@", 1)[0])

    if subject:
        parts.append(_truncate_text(subject, 48))

    if preview:
        parts.append(_truncate_text(preview, 88))

    return " ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· ".join(parts)


def _ao_live_url() -> str:
    landing = str(os.getenv("LANDING_PAGE_URL", "") or "").strip().rstrip("/")
    if not landing:
        return ""
    return f"{landing}/ao_live_robinhood_desktop.html"


def _window_status_label(outreach_digest: dict[str, Any]) -> str:
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    cap = int(outreach_digest.get("daily_send_cap", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    is_open = bool(outreach_digest.get("send_window_is_open"))

    if cap > 0 and sent_today >= cap:
        return "CAP HIT"
    if not is_open:
        return "CLOSED"
    if due_now <= 0:
        return "IDLE"
    return "OPEN"


def _window_summary_line(outreach_digest: dict[str, Any]) -> str:
    label = _window_status_label(outreach_digest)
    start_local = str(outreach_digest.get("send_window_start_local") or "").strip()
    end_local = str(outreach_digest.get("send_window_end_local") or "").strip()
    tz = str(outreach_digest.get("send_window_timezone") or "America/New_York").strip()

    if start_local and end_local:
        try:
            start_dt = datetime.fromisoformat(start_local)
            end_dt = datetime.fromisoformat(end_local)
            hours = f"{start_dt.strftime('%I:%M %p')}ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“{end_dt.strftime('%I:%M %p')}"
        except Exception:
            hours = "window unknown"
    else:
        hours = "window unknown"

    cap = int(outreach_digest.get("daily_send_cap", 0) or 0)
    return f"{label} ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· {hours} ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· {tz} ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· cap {cap}"


def _email_activity_summary_html(outreach_digest: dict[str, Any], limit: int = 3) -> str:
    sent = outreach_digest.get("recent_sent", [])[:limit]
    replies = outreach_digest.get("recent_replies", [])[:limit]
    reply_to_email = str(outreach_digest.get("reply_to_email") or "").strip() or "missing"
    reply_mailbox = str(outreach_digest.get("reply_mailbox_address") or "").strip() or "missing"

    def render_lines(items: list[dict[str, Any]], *, incoming: bool = False, empty: str = "none yet") -> str:
        if not items:
            return f"<div style=\"color:#667085;font-size:14px;line-height:1.45\">{escape(empty)}</div>"
        return "".join(
            f"<div style=\"padding:8px 0;border-top:1px solid #eaecf0;font-size:14px;line-height:1.45\">{escape(_compact_activity_line(item, incoming=incoming))}</div>"
            for item in items
        )

    return (
        "<div style=\"margin-top:18px\">"
        "<div style=\"font-size:22px;font-weight:800;letter-spacing:-0.02em;margin:0 0 12px\">Email routing</div>"
        f"<div style=\"font-size:14px;line-height:1.5;color:#344054\"><strong>Reply-To:</strong> {escape(reply_to_email)}</div>"
        f"<div style=\"font-size:14px;line-height:1.5;color:#344054;margin-top:4px\"><strong>IMAP reply mailbox:</strong> {escape(reply_mailbox)}</div>"
        "<div style=\"font-size:22px;font-weight:800;letter-spacing:-0.02em;margin:22px 0 12px\">Recent sent</div>"
        f"{render_lines(sent, incoming=False)}"
        "<div style=\"font-size:22px;font-weight:800;letter-spacing:-0.02em;margin:22px 0 12px\">Recent replies</div>"
        f"{render_lines(replies, incoming=True)}"
        "</div>"
    )


def _email_activity_summary_text(outreach_digest: dict[str, Any], limit: int = 3) -> list[str]:
    sent = outreach_digest.get("recent_sent", [])[:limit]
    replies = outreach_digest.get("recent_replies", [])[:limit]
    lines = [
        f"Reply-To: {str(outreach_digest.get('reply_to_email') or '').strip() or 'missing'}",
        f"IMAP reply mailbox: {str(outreach_digest.get('reply_mailbox_address') or '').strip() or 'missing'}",
        "",
        "Recent sent:",
    ]
    if sent:
        lines.extend(f"- {_compact_activity_line(item)}" for item in sent)
    else:
        lines.append("- none yet")
    lines.extend(["", "Recent replies:"])
    if replies:
        lines.extend(f"- {_compact_activity_line(item, incoming=True)}" for item in replies)
    else:
        lines.append("- none yet")
    return lines


def _simple_read_lines(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> list[str]:
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    replies_today = int(outreach_digest.get("replies_today", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    today = summary["today"]
    week = summary["week"]

    if sent_today <= 0:
        health = "Sending looks stalled today."
    elif replies_today > 0:
        health = "Sending is running and you have live reply signal."
    else:
        health = "Sending is running. No reply signal yet."

    revenue_line = (
        f"Money today: ${today['gross_usd']} from {today['payments_count']} payments. "
        f"Last 7 days: ${week['gross_usd']} from {week['payments_count']} payments."
    )

    queue_line = (
        f"Emails sent today: {sent_today}. Replies today: {replies_today}. "
        f"Leads due now: {due_now}. Sender split: {_format_sender_counts(outreach_digest.get('sender_counts_today', {}))}."
    )

    return [health, queue_line, revenue_line]


def _recommendation_paragraph(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    replies_today = int(outreach_digest.get("replies_today", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    week_payments = int(summary["week"].get("payments_count", 0) or 0)
    buyers_waiting = int(summary["today"].get("buyers_waiting_on_intake", 0) or 0)

    if replies_today > 0:
        return (
            "Best next move: work live replies fast and learn from them before changing anything else. "
            "Keep the current volume flat, answer interested people quickly, and tighten the follow-up around the objections they actually raise."
        )

    if buyers_waiting > 0:
        return (
            "Best next move: close the loop on people who already paid or still owe intake. "
            "That is the cleanest, most realistic path to more revenue right now, and it improves autonomy without getting scammy."
        )

    if sent_today <= 0:
        return (
            "Best next move: sending did not happen today, so do not rewrite the offer or touch growth plans yet. "
            "The only priority is to confirm the daily send actually ran before judging the market."
        )

    if sent_today > 0 and replies_today == 0 and week_payments > 0:
        return (
            "Best next move: let a few clean send days accumulate before changing the machine. "
            "You already proved somebody will pay, so the realistic path is better reply rate, not more complexity. "
            "If replies stay at zero after 2ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“3 clean days, improve targeting and opener clarity before you touch anything else."
        )

    if sent_today > 0 and replies_today == 0 and due_now > 0:
        return (
            "Best next move: stay boring and legal. Keep the low-volume senders running, do not scale, and do not thrash the site or product. "
            "If there is still zero reply signal after 2ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“3 clean days, improve targeting and opener clarity before making any bigger changes."
        )

    return (
        "Best next move: keep the machine simple, keep the claim believable, and only change one thing at a time. "
        "The safest route to more money is better reply quality and faster handling of real interest, not more volume or more promises."
    )


def _daily_update_subject(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary["today"]
    return (
        f"[AO Update] "
        f"r{outreach_digest.get('replies_today', 0)} "
        f"${today['gross_usd']} "
        f"s{outreach_digest.get('sent_today', 0)} "
        f"q{outreach_digest.get('due_now_count', 0)}"
    )

def _daily_update_html(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_paragraph(summary, outreach_digest)
    ao_live_url = _ao_live_url()
    compact_sender_split = _compact_sender_counts(outreach_digest.get("sender_counts_today", {}))
    cards = [
        ("1. Replies", str(outreach_digest.get("replies_today", 0)), "today", "Watch this first."),
        ("2. Money", f"${today['gross_usd']}", f"{today['payments_count']} payments today", f"Week ${week['gross_usd']} ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· Month ${month['gross_usd']}"),
        ("3. Sends", str(outreach_digest.get("sent_today", 0)), f"{outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap", compact_sender_split),
        ("4. Queue", str(outreach_digest.get("due_now_count", 0)), "due now", f"{outreach_digest.get('in_sequence_count', 0)} in sequence"),
        ("5. Window / cap", _window_status_label(outreach_digest), _window_summary_line(outreach_digest), ""),
    ]

    card_html = "".join(
        f"""
        <div style="margin:0 0 12px;padding:16px 18px;border:1px solid #eaecf0;border-radius:18px;background:#ffffff;">
          <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#667085;margin-bottom:8px;">{escape(label)}</div>
          <div style="font-size:34px;line-height:1;font-weight:900;letter-spacing:-0.05em;color:#101828;margin-bottom:8px;">{escape(value)}</div>
          <div style="font-size:14px;line-height:1.45;color:#344054;">{escape(sub)}</div>
          {f'<div style="font-size:13px;line-height:1.45;color:#667085;margin-top:6px;">{escape(foot)}</div>' if foot else ''}
        </div>
        """
        for label, value, sub, foot in cards
    )

    live_button = (
        f"""
        <div style="margin:18px 0 0;">
          <a href="{escape(ao_live_url)}" style="display:inline-block;background:#16a34a;color:#ffffff;text-decoration:none;padding:12px 16px;border-radius:12px;font-weight:800;">Open AO Live</a>
        </div>
        """
        if ao_live_url
        else ""
    )

    return f"""
<div style="margin:0;padding:0;background:#f8fafc;">
  <div style="max-width:680px;margin:0 auto;padding:20px 14px;font-family:Arial,Helvetica,sans-serif;color:#101828;line-height:1.5;">
    <div style="background:#ffffff;border:1px solid #eaecf0;border-radius:24px;padding:20px 16px;">
      <div style="font-size:34px;line-height:1;font-weight:900;letter-spacing:-0.05em;margin:0 0 8px;">AO Update</div>
      <div style="font-size:14px;color:#667085;">Seed check sent separately: {escape(seed_result.get('subject', '[AO Seed Check]'))}</div>

      <div style="margin:18px 0 8px;font-size:22px;font-weight:800;letter-spacing:-0.02em;">The 5 things to watch</div>
      {card_html}

      <div style="margin:18px 0 8px;font-size:22px;font-weight:800;letter-spacing:-0.02em;">Best next move</div>
      <div style="font-size:16px;line-height:1.6;color:#344054;">{escape(recommendation)}</div>

      {_email_activity_summary_html(outreach_digest, limit=3)}

      {live_button}
    </div>
  </div>
</div>
""".strip()

def _daily_update_text(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_paragraph(summary, outreach_digest)
    ao_live_url = _ao_live_url()

    lines = [
        "AO Update",
        "",
        f"Seed check sent separately: {seed_result.get('subject', '[AO Seed Check]')}",
        "",
        "The 5 things to watch:",
        f"1. Replies: {outreach_digest.get('replies_today', 0)} today",
        f"2. Money: ${today['gross_usd']} today from {today['payments_count']} payments",
        f"   Week ${week['gross_usd']} ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· Month ${month['gross_usd']}",
        f"3. Sends: {outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap",
        f"   Sender split: {_compact_sender_counts(outreach_digest.get('sender_counts_today', {}))}",
        f"4. Queue: {outreach_digest.get('due_now_count', 0)} due now",
        f"   {outreach_digest.get('in_sequence_count', 0)} in sequence",
        f"5. Window / cap: {_window_summary_line(outreach_digest)}",
        "",
        "Best next move:",
        recommendation,
        "",
        *_email_activity_summary_text(outreach_digest, limit=3),
    ]
    if ao_live_url:
        lines.extend(["", f"AO Live: {ao_live_url}"])
    return "\n".join(lines)

def send_daily_money_summary() -> dict[str, Any]:
    summary = money_summary()
    outreach_digest = outreach_status()

    seed_result = _send_seed_check_email()

    update_subject = _daily_update_subject(summary, outreach_digest)
    update_html = _daily_update_html(summary, outreach_digest, seed_result)
    update_text = _daily_update_text(summary, outreach_digest, seed_result)
    update_send_result = _send_smtp_email_from_seed_sender(
        subject=update_subject,
        text_body=update_text,
        html_body=update_html,
    )

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type="daily_ops_update_sent",
                prospect_external_id="ops",
                summary=update_subject,
                payload_json=json.dumps(
                    {
                        "money": summary,
                        "outreach_digest": outreach_digest,
                        "seed_result": seed_result,
                        "update_send_result": update_send_result,
                    },
                    ensure_ascii=False,
                ),
            )
        )
        if seed_result.get("status") == "sent":
            session.add(
                AcquisitionEvent(
                    event_type="deliverability_seed_sent",
                    prospect_external_id="ops",
                    summary=seed_result.get("subject", "[AO Seed Check]"),
                    payload_json=json.dumps(seed_result, ensure_ascii=False),
                )
            )
        session.commit()

    return {
        "status": "ok",
        "seed_result": seed_result,
        "update_subject": update_subject,
        "update_send_result": update_send_result,
        "summary": summary,
        "outreach_digest": outreach_digest,
    }


def monthly_summary(days: int = 30) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(days=days)

    with _session() as session:
        rows = (
            session.query(AcquisitionEvent.event_type)
            .filter(AcquisitionEvent.created_at >= cutoff)
            .all()
        )

    event_counts: dict[str, int] = {}
    for (event_type,) in rows:
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

    return {
        "days": days,
        "event_counts": event_counts,
        "outreach_digest": outreach_status(),
        "money": money_summary(),
    }

# --- AO UPDATE MOBILE COMPACT OVERRIDE START ---
def _ascii_safe(value: Any) -> str:
    text = str(value or "")
    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2022": " ",
        "\u00b7": " | ",
        "\u2026": "...",
        "\xa0": " ",
        "Â·": " | ",
        "â€¢": " ",
        "â€“": "-",
        "â€”": "-",
        "â€¦": "...",
        "Â": "",
        "Ã‚": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def _compact_sender_counts_ascii(sender_counts: dict[str, Any]) -> str:
    if not sender_counts:
        return "none yet"
    parts: list[str] = []
    for key, value in sender_counts.items():
        local = str(key).split("@", 1)[0].strip() or str(key)
        parts.append(f"{local} {value}")
    return " | ".join(parts)


def _window_summary_ascii(outreach_digest: dict[str, Any]) -> str:
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    cap = int(outreach_digest.get("daily_send_cap", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    is_open = bool(outreach_digest.get("send_window_is_open"))

    if cap > 0 and sent_today >= cap:
        label = "CAP HIT"
    elif not is_open:
        label = "CLOSED"
    elif due_now <= 0:
        label = "IDLE"
    else:
        label = "OPEN"

    start_local = str(outreach_digest.get("send_window_start_local") or "").strip()
    end_local = str(outreach_digest.get("send_window_end_local") or "").strip()
    tz = str(outreach_digest.get("send_window_timezone") or "America/New_York").strip()

    if start_local and end_local:
        try:
            start_dt = datetime.fromisoformat(start_local)
            end_dt = datetime.fromisoformat(end_local)
            hours = f"{start_dt.strftime('%I:%M %p')}-{end_dt.strftime('%I:%M %p')}"
        except Exception:
            hours = "window unknown"
    else:
        hours = "window unknown"

    return _ascii_safe(f"{label} | {hours} | {tz} | cap {cap}")


def _recommendation_ascii(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    replies_today = int(outreach_digest.get("replies_today", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    buyers_waiting = int(summary["today"].get("buyers_waiting_on_intake", 0) or 0)

    if replies_today > 0:
        return "Reply signal exists. Keep volume flat, answer real interest quickly, and learn from objections before changing anything else."

    if buyers_waiting > 0:
        return "Close the loop on people who already paid or still owe intake. That is the cleanest path to more revenue right now."

    if sent_today <= 0:
        return "Sending did not happen today. First confirm the daily send actually ran before judging the market."

    if sent_today > 0 and due_now > 0:
        return "Stay boring and legal. Keep the low-volume senders running. Do not scale. If reply signal is still zero after 2-3 clean days, improve targeting and opener clarity before bigger changes."

    return "Keep the machine simple. Change one thing at a time. The next gain should come from better reply quality, not more complexity."


def _daily_update_subject(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    replies_today = int(outreach_digest.get("replies_today", 0) or 0)
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    money_today = summary["today"].get("gross_usd", 0)
    return _ascii_safe(f"[AO Update] r{replies_today} ${money_today} s{sent_today} q{due_now}")


def _daily_update_html(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_ascii(summary, outreach_digest)
    ao_live_url = str(_ao_live_url() or "").strip()
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))

    rows = [
        ("1. Replies", _ascii_safe(f"{outreach_digest.get('replies_today', 0)} today"), "Watch this first."),
        ("2. Money", _ascii_safe(f"${today['gross_usd']} today | {today['payments_count']} payments"), _ascii_safe(f"Week ${week['gross_usd']} | Month ${month['gross_usd']}")),
        ("3. Sends", _ascii_safe(f"{outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap"), _compact_sender_counts_ascii(outreach_digest.get("sender_counts_today", {}))),
        ("4. Queue", _ascii_safe(f"{outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"), ""),
        ("5. Window / cap", _window_summary_ascii(outreach_digest), ""),
    ]

    row_html = "".join(
        f"""
        <div style="padding:{'12px 14px' if i == 0 else '12px 14px'};{'border-top:1px solid #2b3240;' if i > 0 else ''}">
          <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#98a2b3;margin-bottom:4px;">{escape(_ascii_safe(label))}</div>
          <div style="font-size:15px;line-height:1.45;color:#f8fafc;font-weight:800;">{escape(_ascii_safe(main))}</div>
          {f'<div style="font-size:13px;line-height:1.45;color:#98a2b3;margin-top:4px;">{escape(_ascii_safe(sub))}</div>' if sub else ''}
        </div>
        """
        for i, (label, main, sub) in enumerate(rows)
    )

    live_button = (
        f"""
        <div style="margin:16px 0 0;">
          <a href="{escape(ao_live_url)}" style="display:inline-block;background:#16a34a;color:#ffffff;text-decoration:none;padding:11px 15px;border-radius:12px;font-weight:800;">Open AO Live</a>
        </div>
        """
        if ao_live_url
        else ""
    )

    return f"""
<div style="margin:0;padding:8px;background:#0f1115;">
  <div style="max-width:620px;margin:0 auto;font-family:Arial,Helvetica,sans-serif;color:#f8fafc;line-height:1.5;">
    <div style="background:#181c23;border:1px solid #2b3240;border-radius:18px;padding:16px 14px;">
      <div style="font-size:30px;line-height:1.05;font-weight:900;letter-spacing:-0.04em;margin:0 0 8px;">AO Update</div>
      <div style="font-size:13px;line-height:1.45;color:#98a2b3;">Seed check sent separately: {escape(seed_subject)}</div>

      <div style="margin:16px 0 10px;font-size:18px;font-weight:850;letter-spacing:-0.02em;">The 5 things to watch</div>
      <div style="border:1px solid #2b3240;border-radius:16px;background:#0b0d12;overflow:hidden;">
        {row_html}
      </div>

      <div style="margin:16px 0 8px;font-size:18px;font-weight:850;letter-spacing:-0.02em;">Best next move</div>
      <div style="font-size:15px;line-height:1.6;color:#d0d5dd;">{escape(recommendation)}</div>

      {live_button}
    </div>
  </div>
</div>
""".strip()


def _daily_update_text(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_ascii(summary, outreach_digest)
    ao_live_url = str(_ao_live_url() or "").strip()
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))

    lines = [
        "AO Update",
        "",
        f"Seed check sent separately: {seed_subject}",
        "",
        "The 5 things to watch:",
        _ascii_safe(f"1. Replies: {outreach_digest.get('replies_today', 0)} today"),
        _ascii_safe(f"2. Money: ${today['gross_usd']} today | {today['payments_count']} payments"),
        _ascii_safe(f"   Week ${week['gross_usd']} | Month ${month['gross_usd']}"),
        _ascii_safe(f"3. Sends: {outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap | {_compact_sender_counts_ascii(outreach_digest.get('sender_counts_today', {}))}"),
        _ascii_safe(f"4. Queue: {outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"),
        _ascii_safe(f"5. Window / cap: {_window_summary_ascii(outreach_digest)}"),
        "",
        "Best next move:",
        recommendation,
    ]

    if ao_live_url:
        lines.extend(["", f"AO Live: {ao_live_url}"])

    return "\n".join(lines)
# --- AO UPDATE MOBILE COMPACT OVERRIDE END ---


# --- AO DUET INTEL SECTION START ---
import base64 as _duet_base64
import urllib.error as _duet_urlerror
import urllib.request as _duet_urlrequest

_DUET_INTEL_CACHE: dict[str, Any] | None = None

if "_ascii_safe" not in globals():
    def _ascii_safe(value: Any) -> str:
        return " ".join(str(value or "").split())


def _duet_env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _duet_base_url() -> str:
    return _duet_env("DUET_APP_BASE_URL", "https://duet.aolabs.io").rstrip("/")


def _duet_admin_url() -> str:
    return f"{_duet_base_url()}/admin"


def _duet_public_url() -> str:
    return _duet_base_url()


def _duet_takeover_line(item: dict[str, Any]) -> str:
    instrument = str(item.get("instrument") or "").lower()
    preview = str(item.get("preview") or "").lower()

    if "violin" in instrument or "violin" in preview:
        return "ok violin is a dangerous answer. what would you actually want to play together"
    if "cello" in instrument or "cello" in preview:
        return "ok cello helps. are you more solo person or quartet person"
    if "piano" in instrument or "piano" in preview:
        return "ok piano helps. accompaniment person or chaos person"
    if "send another" in preview or "sit too close" in preview:
        return "ok that answer is actually very on brand. what made you click this"
    return "ok this is kind of funny. what made you click this"


def _duet_item_score(item: dict[str, Any]) -> int:
    raw = int(item.get("fit_score") or item.get("overall_fit_score") or 0)
    instrument = str(item.get("instrument") or item.get("instrument_primary") or "").lower()
    preview = str(item.get("preview") or item.get("score_notes") or "").lower()
    state = str(item.get("state") or "").lower()

    score = raw
    if "violin" in instrument or "violin" in preview:
        score += 35
    elif any(x in instrument or x in preview for x in ["viola", "cello", "piano", "voice"]):
        score += 18

    if any(x in preview for x in ["send another", "this is why i like you", "sit too close", "duet + boba", "dramatic glare", "mess up louder"]):
        score += 18

    if any(x in preview for x in ["ig", "@", "music taste", "bach", "orchestra", "quartet"]):
        score += 8

    if state in {"handoff_ready", "warm"}:
        score += 20
    if state in {"engaged", "awaiting_reply"}:
        score += 10

    return max(0, min(100, score))


def _duet_fetch_dashboard() -> dict[str, Any]:
    base = _duet_base_url()
    if not base:
        return {"status": "disabled", "reason": "DUET_APP_BASE_URL missing"}

    url = f"{base}/ui/dashboard-data"
    req = _duet_urlrequest.Request(url, headers={"User-Agent": "alan-operator-duet-intel/1.0"})

    username = _duet_env("DUET_ADMIN_USERNAME")
    password = _duet_env("DUET_ADMIN_PASSWORD")
    if username and password:
        token = _duet_base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")

    try:
        with _duet_urlrequest.urlopen(req, timeout=float(_duet_env("DUET_FETCH_TIMEOUT_SECONDS", "8") or "8")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return {"status": "ok", "base_url": base, "data": payload}
    except _duet_urlerror.HTTPError as exc:
        if exc.code == 401:
            return {
                "status": "auth_required",
                "base_url": base,
                "reason": "Set DUET_ADMIN_USERNAME and DUET_ADMIN_PASSWORD in AO Railway to match the duet app admin login.",
            }
        return {"status": "http_error", "base_url": base, "code": exc.code, "reason": str(exc)}
    except Exception as exc:
        return {"status": "error", "base_url": base, "reason": str(exc)}


def _duet_intel() -> dict[str, Any]:
    global _DUET_INTEL_CACHE
    if _DUET_INTEL_CACHE is not None:
        return _DUET_INTEL_CACHE

    fetched = _duet_fetch_dashboard()
    status = fetched.get("status")
    base = fetched.get("base_url") or _duet_base_url()
    admin_url = f"{base}/admin" if base else ""
    public_url = base or ""

    if status != "ok":
        _DUET_INTEL_CACHE = {
            "status": status,
            "base_url": base,
            "admin_url": admin_url,
            "public_url": public_url,
            "summary": {},
            "hot": [],
            "waiting": [],
            "top_candidates": [],
            "best_action": fetched.get("reason") or "Duet Intel could not pull the dashboard yet.",
            "reason": fetched.get("reason") or fetched.get("code") or status,
        }
        return _DUET_INTEL_CACHE

    data = fetched.get("data", {})
    conversations = list(data.get("conversations") or [])
    top_candidates = list(data.get("top_candidates") or [])
    summary = dict(data.get("summary") or {})

    scored: list[dict[str, Any]] = []
    for item in conversations:
        enriched = dict(item)
        enriched["duet_score"] = _duet_item_score(enriched)
        enriched["takeover_line"] = _duet_takeover_line(enriched)
        scored.append(enriched)

    hot = sorted(
        scored,
        key=lambda row: (int(row.get("duet_score") or 0), str(row.get("last_activity_at") or "")),
        reverse=True,
    )[:3]

    waiting = [
        x for x in scored
        if str(x.get("state") or "").lower() in {"engaged", "awaiting_reply", "warm", "handoff_ready"}
    ][:5]

    active = int(summary.get("active_threads") or len(conversations) or 0)
    hot_count = sum(1 for x in scored if int(x.get("duet_score") or 0) >= 70)
    violin_count = sum(
        1 for x in scored
        if "violin" in str(x.get("instrument") or "").lower()
        or "violin" in str(x.get("preview") or "").lower()
    )

    if hot:
        first = hot[0]
        best_action = f"Open {first.get('candidate_name') or 'top duet lead'} and consider: {first.get('takeover_line')}"
    elif active > 0:
        best_action = "A few duet submissions exist, but none are screaming hot. Let the screener run and only take over if a reply feels genuinely warm."
    else:
        best_action = "No duet submissions yet. Best move: use duet.aolabs.io only in a relevant music/flirty context, not as a public blast."

    _DUET_INTEL_CACHE = {
        "status": "ok",
        "base_url": base,
        "admin_url": admin_url,
        "public_url": public_url,
        "summary": summary,
        "active_threads": active,
        "hot_count": hot_count,
        "violin_count": violin_count,
        "hot": hot,
        "waiting": waiting,
        "top_candidates": top_candidates,
        "best_action": best_action,
    }
    return _DUET_INTEL_CACHE


def _duet_html() -> str:
    intel = _duet_intel()
    status = str(intel.get("status") or "unknown")
    admin_url = str(intel.get("admin_url") or "")
    public_url = str(intel.get("public_url") or "https://duet.aolabs.io")

    if status != "ok":
        return f"""
      <div style="margin:16px 0 0;padding:14px;border:1px solid #7c2d12;border-radius:16px;background:#2a1714;">
        <div style="font-size:18px;font-weight:900;color:#fed7aa;margin-bottom:6px;">Duet Intel</div>
        <div style="font-size:14px;line-height:1.55;color:#fdba74;">{escape(_ascii_safe(intel.get('best_action') or intel.get('reason') or status))}</div>
        <div style="font-size:13px;line-height:1.45;color:#f7b58a;margin-top:8px;">Public link: {escape(public_url)}</div>
      </div>
        """

    hot = list(intel.get("hot") or [])
    rows = [
        ("Active", str(intel.get("active_threads", 0))),
        ("Hot", str(intel.get("hot_count", 0))),
        ("Violin", str(intel.get("violin_count", 0))),
    ]

    stat_html = "".join(
        f"<div style='padding:10px 12px;border:1px solid #5b2e2b;border-radius:14px;background:#211319;'><div style='font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#f3b7aa'>{escape(label)}</div><div style='font-size:24px;font-weight:950;color:#fff;margin-top:2px'>{escape(value)}</div></div>"
        for label, value in rows
    )

    if hot:
        hot_html = "".join(
            f"""
            <div style="padding:10px 0;border-top:1px solid #4a2525;">
              <div style="font-size:14px;font-weight:900;color:#fff;">{escape(_ascii_safe(item.get('candidate_name') or 'unknown'))} <span style="color:#f3b7aa;">score {escape(str(item.get('duet_score', 0)))}</span></div>
              <div style="font-size:13px;line-height:1.45;color:#f8d5cf;">{escape(_ascii_safe(item.get('instrument') or 'unknown'))} | {escape(_ascii_safe(item.get('state') or ''))}</div>
              <div style="font-size:13px;line-height:1.45;color:#f8d5cf;margin-top:3px;">{escape(_ascii_safe(item.get('preview') or ''))}</div>
              <div style="font-size:13px;line-height:1.45;color:#ffffff;margin-top:4px;"><strong>takeover:</strong> {escape(_ascii_safe(item.get('takeover_line') or ''))}</div>
            </div>
            """
            for item in hot
        )
    else:
        hot_html = "<div style='font-size:14px;line-height:1.5;color:#f8d5cf;padding-top:8px;border-top:1px solid #4a2525;'>No hot duet leads yet.</div>"

    admin_button = f"<a href='{escape(admin_url)}' style='display:inline-block;background:#f08f78;color:#24120f;text-decoration:none;padding:10px 13px;border-radius:12px;font-weight:900;margin-right:8px;'>Open Duet Admin</a>" if admin_url else ""
    public_button = f"<a href='{escape(public_url)}' style='display:inline-block;background:#fff7f3;color:#7c2d12;text-decoration:none;padding:10px 13px;border-radius:12px;font-weight:900;'>Open Public Page</a>"

    return f"""
      <div style="margin:16px 0 0;padding:14px;border:1px solid #5b2e2b;border-radius:16px;background:#351a1e;">
        <div style="font-size:18px;font-weight:900;color:#fff;margin-bottom:8px;">Duet Intel</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px;">{stat_html}</div>
        <div style="font-size:14px;line-height:1.55;color:#ffe4df;margin-bottom:8px;"><strong>Best move:</strong> {escape(_ascii_safe(intel.get('best_action') or ''))}</div>
        {hot_html}
        <div style="margin-top:12px;">{admin_button}{public_button}</div>
      </div>
    """


def _duet_text_lines() -> list[str]:
    intel = _duet_intel()
    lines = ["", "Duet Intel:"]
    if intel.get("status") != "ok":
        lines.append(f"- status: {_ascii_safe(intel.get('status'))}")
        lines.append(f"- action: {_ascii_safe(intel.get('best_action') or intel.get('reason'))}")
        lines.append(f"- public: {_ascii_safe(intel.get('public_url') or 'https://duet.aolabs.io')}")
        return lines

    lines.append(_ascii_safe(f"- active: {intel.get('active_threads', 0)} | hot: {intel.get('hot_count', 0)} | violin: {intel.get('violin_count', 0)}"))
    lines.append(_ascii_safe(f"- best move: {intel.get('best_action', '')}"))

    hot = list(intel.get("hot") or [])
    if hot:
        lines.append("- hot leads:")
        for item in hot[:3]:
            lines.append(_ascii_safe(f"  - {item.get('candidate_name', 'unknown')} | score {item.get('duet_score', 0)} | {item.get('instrument', '')} | {item.get('state', '')}"))
            lines.append(_ascii_safe(f"    takeover: {item.get('takeover_line', '')}"))
    else:
        lines.append("- hot leads: none yet")

    if intel.get("admin_url"):
        lines.append(_ascii_safe(f"- admin: {intel.get('admin_url')}"))
    if intel.get("public_url"):
        lines.append(_ascii_safe(f"- public: {intel.get('public_url')}"))
    return lines


def _daily_update_subject(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    replies_today = int(outreach_digest.get("replies_today", 0) or 0)
    sent_today = int(outreach_digest.get("sent_today", 0) or 0)
    due_now = int(outreach_digest.get("due_now_count", 0) or 0)
    money_today = summary["today"].get("gross_usd", 0)
    duet = _duet_intel()
    duet_bits = ""
    if duet.get("status") == "ok":
        duet_bits = f" d{duet.get('hot_count', 0)}/{duet.get('active_threads', 0)}"
    return _ascii_safe(f"[AO Update] r{replies_today} ${money_today} s{sent_today} q{due_now}{duet_bits}")


def _daily_update_html(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_ascii(summary, outreach_digest)
    ao_live_url = str(_ao_live_url() or "").strip()
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))

    rows = [
        ("1. Replies", _ascii_safe(f"{outreach_digest.get('replies_today', 0)} today"), "Watch this first."),
        ("2. Money", _ascii_safe(f"${today['gross_usd']} today | {today['payments_count']} payments"), _ascii_safe(f"Week ${week['gross_usd']} | Month ${month['gross_usd']}")),
        ("3. Sends", _ascii_safe(f"{outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap"), _compact_sender_counts_ascii(outreach_digest.get("sender_counts_today", {}))),
        ("4. Queue", _ascii_safe(f"{outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"), ""),
        ("5. Window / cap", _window_summary_ascii(outreach_digest), ""),
    ]

    row_html = "".join(
        f"""
        <div style="padding:12px 14px;{'border-top:1px solid #2b3240;' if i > 0 else ''}">
          <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#98a2b3;margin-bottom:4px;">{escape(_ascii_safe(label))}</div>
          <div style="font-size:15px;line-height:1.45;color:#f8fafc;font-weight:800;">{escape(_ascii_safe(main))}</div>
          {f'<div style="font-size:13px;line-height:1.45;color:#98a2b3;margin-top:4px;">{escape(_ascii_safe(sub))}</div>' if sub else ''}
        </div>
        """
        for i, (label, main, sub) in enumerate(rows)
    )

    live_button = (
        f"""
        <div style="margin:16px 0 0;">
          <a href="{escape(ao_live_url)}" style="display:inline-block;background:#16a34a;color:#ffffff;text-decoration:none;padding:11px 15px;border-radius:12px;font-weight:800;">Open AO Live</a>
        </div>
        """
        if ao_live_url
        else ""
    )

    return f"""
<div style="margin:0;padding:8px;background:#0f1115;">
  <div style="max-width:620px;margin:0 auto;font-family:Arial,Helvetica,sans-serif;color:#f8fafc;line-height:1.5;">
    <div style="background:#181c23;border:1px solid #2b3240;border-radius:18px;padding:16px 14px;">
      <div style="font-size:30px;line-height:1.05;font-weight:900;letter-spacing:-0.04em;margin:0 0 8px;">AO Update</div>
      <div style="font-size:13px;line-height:1.45;color:#98a2b3;">Seed check sent separately: {escape(seed_subject)}</div>

      <div style="margin:16px 0 10px;font-size:18px;font-weight:850;letter-spacing:-0.02em;">The 5 things to watch</div>
      <div style="border:1px solid #2b3240;border-radius:16px;background:#0b0d12;overflow:hidden;">
        {row_html}
      </div>

      {_duet_html()}

      <div style="margin:16px 0 8px;font-size:18px;font-weight:850;letter-spacing:-0.02em;">Best next move</div>
      <div style="font-size:15px;line-height:1.6;color:#d0d5dd;">{escape(recommendation)}</div>

      {live_button}
    </div>
  </div>
</div>
""".strip()


def _daily_update_text(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary["today"]
    week = summary["week"]
    month = summary["month"]
    recommendation = _recommendation_ascii(summary, outreach_digest)
    ao_live_url = str(_ao_live_url() or "").strip()
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))

    lines = [
        "AO Update",
        "",
        f"Seed check sent separately: {seed_subject}",
        "",
        "The 5 things to watch:",
        _ascii_safe(f"1. Replies: {outreach_digest.get('replies_today', 0)} today"),
        _ascii_safe(f"2. Money: ${today['gross_usd']} today | {today['payments_count']} payments"),
        _ascii_safe(f"   Week ${week['gross_usd']} | Month ${month['gross_usd']}"),
        _ascii_safe(f"3. Sends: {outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)} cap | {_compact_sender_counts_ascii(outreach_digest.get('sender_counts_today', {}))}"),
        _ascii_safe(f"4. Queue: {outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"),
        _ascii_safe(f"5. Window / cap: {_window_summary_ascii(outreach_digest)}"),
    ]

    lines.extend(_duet_text_lines())
    lines.extend(["", "Best next move:", recommendation])

    if ao_live_url:
        lines.extend(["", f"AO Live: {ao_live_url}"])

    return "\n".join(lines)


def send_daily_money_summary() -> dict[str, Any]:
    global _DUET_INTEL_CACHE
    _DUET_INTEL_CACHE = None

    summary = money_summary()
    outreach_digest = outreach_status()
    duet_intel = _duet_intel()

    seed_result = _send_seed_check_email()

    update_subject = _daily_update_subject(summary, outreach_digest)
    update_html = _daily_update_html(summary, outreach_digest, seed_result)
    update_text = _daily_update_text(summary, outreach_digest, seed_result)
    update_send_result = _send_smtp_email_from_seed_sender(
        subject=update_subject,
        text_body=update_text,
        html_body=update_html,
    )

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type="daily_ops_update_sent",
                prospect_external_id="ops",
                summary=update_subject,
                payload_json=json.dumps(
                    {
                        "money": summary,
                        "outreach_digest": outreach_digest,
                        "duet_intel": duet_intel,
                        "seed_result": seed_result,
                        "update_send_result": update_send_result,
                    },
                    ensure_ascii=False,
                ),
            )
        )
        if seed_result.get("status") == "sent":
            session.add(
                AcquisitionEvent(
                    event_type="deliverability_seed_sent",
                    prospect_external_id="ops",
                    summary=seed_result.get("subject", "[AO Seed Check]"),
                    payload_json=json.dumps(seed_result, ensure_ascii=False),
                )
            )
        session.commit()

    return {
        "status": "ok",
        "seed_result": seed_result,
        "update_subject": update_subject,
        "update_send_result": update_send_result,
        "summary": summary,
        "outreach_digest": outreach_digest,
        "duet_intel": duet_intel,
    }
# --- AO DUET INTEL SECTION END ---


# --- AO RELAY DUET SECTIONS START ---

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _relay_html(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})

    rows = [
        ("Money today", f"${today.get('gross_usd', 0)}", f"{today.get('payments_count', 0)} payments"),
        ("Money week", f"${week.get('gross_usd', 0)}", "current week"),
        ("Money month", f"${month.get('gross_usd', 0)}", "current month"),
        ("Emails sent", str(outreach_digest.get("sent_today", 0)), f"cap {outreach_digest.get('daily_send_cap', 0)}"),
        ("Replies", str(outreach_digest.get("replies_today", 0)), "today"),
        ("Queue", str(outreach_digest.get("due_now_count", 0)), f"{outreach_digest.get('in_sequence_count', 0)} in sequence"),
    ]

    cards = "".join(
        f"""
        <div style="padding:12px;border:1px solid #2b3240;border-radius:14px;background:#0b0d12;">
          <div style="font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#98a2b3;">{escape(_ascii_safe(label))}</div>
          <div style="font-size:25px;font-weight:950;color:#fff;margin-top:2px;">{escape(_ascii_safe(main))}</div>
          <div style="font-size:12px;color:#98a2b3;margin-top:2px;">{escape(_ascii_safe(sub))}</div>
        </div>
        """
        for label, main, sub in rows
    )

    recommendation = _recommendation_ascii(summary, outreach_digest)

    return f"""
      <div style="margin:16px 0 0;padding:16px;border:1px solid #2b3240;border-radius:18px;background:#11151d;">
        <div style="font-size:22px;font-weight:950;color:#fff;margin-bottom:4px;">Relay</div>
        <div style="font-size:13px;color:#98a2b3;margin-bottom:12px;">money, leads, email, queue</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px;">{cards}</div>
        <div style="font-size:14px;line-height:1.55;color:#d0d5dd;"><strong style="color:#fff;">Best Relay move:</strong> {escape(_ascii_safe(recommendation))}</div>
      </div>
    """


def _duet_opportunities() -> list[dict[str, str]]:
    # This is deliberately source-first, not creepy-person-first.
    # Goal: find places where compatible musician people concentrate.
    base_queries = [
        {
            "title": "Boston chamber / classical events",
            "why": "Highest density of serious musicians and music-adjacent people near you.",
            "move": "Pick one event that feels non-stuffy. If a music convo happens, use: wait take this tiny duet test lol",
            "url": "https://www.google.com/search?q=Boston+chamber+music+events+this+week",
        },
        {
            "title": "Worcester music events",
            "why": "Local enough to actually go. Better odds than broad internet searching.",
            "move": "Find one casual recital/open event. Go as yourself, not as a networking robot.",
            "url": "https://www.google.com/search?q=Worcester+MA+classical+music+recital+events+this+week",
        },
        {
            "title": "Adult music classes / ensembles",
            "why": "Best structural fit: adults who already play or want to play instruments.",
            "move": "Look for violin, chamber, orchestra, piano, voice, or adult ensemble programs.",
            "url": "https://www.google.com/search?q=Boston+adult+violin+chamber+music+class",
        },
        {
            "title": "NEC / Berklee / conservatory public events",
            "why": "Performer energy, music identity, concerts, recitals, friend-of-musician crowd.",
            "move": "Scan public calendars. Choose one event where talking after would not be weird.",
            "url": "https://www.google.com/search?q=NEC+Berklee+public+recitals+Boston+this+week",
        },
        {
            "title": "Meetup / Eventbrite musician social events",
            "why": "More explicitly social. Lower barrier than formal concert spaces.",
            "move": "Find one casual music meetup, jam, open mic, or listening event.",
            "url": "https://www.google.com/search?q=Boston+violin+music+meetup+eventbrite",
        },
    ]

    return base_queries[:5]


def _duet_opportunity_html() -> str:
    items = _duet_opportunities()
    html = ""

    for i, item in enumerate(items[:5], start=1):
        html += f"""
        <div style="padding:11px 0;border-top:1px solid rgba(255,255,255,.13);">
          <div style="font-size:14px;font-weight:950;color:#fff;">{i}. {escape(_ascii_safe(item.get('title')))}</div>
          <div style="font-size:13px;line-height:1.45;color:#f8d5cf;margin-top:3px;">{escape(_ascii_safe(item.get('why')))}</div>
          <div style="font-size:13px;line-height:1.45;color:#fff;margin-top:4px;"><strong>move:</strong> {escape(_ascii_safe(item.get('move')))}</div>
          <div style="font-size:13px;margin-top:5px;"><a href="{escape(item.get('url', ''))}" style="color:#93c5fd;">open search</a></div>
        </div>
        """

    return html


def _duet_section_html() -> str:
    intel = _duet_intel() if "_duet_intel" in globals() else {"status": "missing", "best_action": "Duet Intel function is missing."}
    status = str(intel.get("status") or "unknown")
    admin_url = str(intel.get("admin_url") or "https://duet.aolabs.io/admin")
    public_url = str(intel.get("public_url") or "https://duet.aolabs.io")

    if status == "ok":
        hot = list(intel.get("hot") or [])
        stat_rows = [
            ("Active", str(intel.get("active_threads", 0))),
            ("Hot", str(intel.get("hot_count", 0))),
            ("Violin", str(intel.get("violin_count", 0))),
        ]

        stat_html = "".join(
            f"""
            <div style="padding:10px 12px;border:1px solid #5b2e2b;border-radius:14px;background:#211319;">
              <div style="font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#f3b7aa;">{escape(label)}</div>
              <div style="font-size:24px;font-weight:950;color:#fff;margin-top:2px;">{escape(value)}</div>
            </div>
            """
            for label, value in stat_rows
        )

        if hot:
            hot_html = "".join(
                f"""
                <div style="padding:10px 0;border-top:1px solid rgba(255,255,255,.13);">
                  <div style="font-size:14px;font-weight:950;color:#fff;">{escape(_ascii_safe(item.get('candidate_name') or 'unknown'))} <span style="color:#f3b7aa;">score {escape(str(item.get('duet_score', 0)))}</span></div>
                  <div style="font-size:13px;line-height:1.45;color:#f8d5cf;">{escape(_ascii_safe(item.get('instrument') or 'unknown'))} | {escape(_ascii_safe(item.get('state') or ''))}</div>
                  <div style="font-size:13px;line-height:1.45;color:#f8d5cf;margin-top:3px;">{escape(_ascii_safe(item.get('preview') or ''))}</div>
                  <div style="font-size:13px;line-height:1.45;color:#fff;margin-top:4px;"><strong>takeover:</strong> {escape(_ascii_safe(item.get('takeover_line') or ''))}</div>
                </div>
                """
                for item in hot[:3]
            )
        else:
            hot_html = "<div style='font-size:14px;line-height:1.5;color:#f8d5cf;padding-top:8px;border-top:1px solid rgba(255,255,255,.13);'>No hot duet leads yet.</div>"

        intel_html = f"""
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px;">{stat_html}</div>
          <div style="font-size:14px;line-height:1.55;color:#ffe4df;margin-bottom:8px;"><strong>Best Duet move:</strong> {escape(_ascii_safe(intel.get('best_action') or ''))}</div>
          {hot_html}
        """
    else:
        intel_html = f"""
          <div style="padding:12px;border:1px solid #7c2d12;border-radius:14px;background:#2a1714;color:#fdba74;font-size:14px;line-height:1.55;">
            {escape(_ascii_safe(intel.get('best_action') or intel.get('reason') or status))}
          </div>
        """

    return f"""
      <div style="margin:16px 0 0;padding:16px;border:1px solid #7c3f2d;border-radius:18px;background:#351a1e;">
        <div style="font-size:22px;font-weight:950;color:#fff;margin-bottom:4px;">Duet</div>
        <div style="font-size:13px;color:#f3b7aa;margin-bottom:12px;">wife funnel, musician environments, hot leads</div>

        <div style="font-size:16px;font-weight:900;color:#fff;margin:8px 0 8px;">Duet Intel</div>
        {intel_html}

        <div style="font-size:16px;font-weight:900;color:#fff;margin:18px 0 8px;">Opportunity Finder</div>
        {_duet_opportunity_html()}

        <div style="margin-top:12px;">
          <a href="{escape(admin_url)}" style="display:inline-block;background:#f08f78;color:#24120f;text-decoration:none;padding:10px 13px;border-radius:12px;font-weight:900;margin-right:8px;">Open Duet Admin</a>
          <a href="{escape(public_url)}" style="display:inline-block;background:#fff7f3;color:#7c2d12;text-decoration:none;padding:10px 13px;border-radius:12px;font-weight:900;">Open Duet Page</a>
        </div>
      </div>
    """


def _duet_section_text_lines() -> list[str]:
    intel = _duet_intel() if "_duet_intel" in globals() else {"status": "missing", "best_action": "Duet Intel function missing."}
    lines = ["", "Duet:"]

    if intel.get("status") == "ok":
        lines.append(_ascii_safe(f"- active: {intel.get('active_threads', 0)} | hot: {intel.get('hot_count', 0)} | violin: {intel.get('violin_count', 0)}"))
        lines.append(_ascii_safe(f"- best move: {intel.get('best_action', '')}"))

        hot = list(intel.get("hot") or [])
        if hot:
            lines.append("- hot leads:")
            for item in hot[:3]:
                lines.append(_ascii_safe(f"  - {item.get('candidate_name', 'unknown')} | score {item.get('duet_score', 0)} | {item.get('instrument', '')}"))
                lines.append(_ascii_safe(f"    takeover: {item.get('takeover_line', '')}"))
        else:
            lines.append("- hot leads: none yet")
    else:
        lines.append(_ascii_safe(f"- status: {intel.get('status')}"))
        lines.append(_ascii_safe(f"- action: {intel.get('best_action') or intel.get('reason')}"))

    lines.append("")
    lines.append("Duet opportunity finder:")
    for i, item in enumerate(_duet_opportunities()[:5], start=1):
        lines.append(_ascii_safe(f"{i}. {item.get('title')}"))
        lines.append(_ascii_safe(f"   why: {item.get('why')}"))
        lines.append(_ascii_safe(f"   move: {item.get('move')}"))
        lines.append(_ascii_safe(f"   link: {item.get('url')}"))

    return lines


def _daily_update_html(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))
    ao_live_url = str(_ao_live_url() or "").strip()

    live_button = (
        f"""
        <div style="margin:16px 0 0;">
          <a href="{escape(ao_live_url)}" style="display:inline-block;background:#16a34a;color:#ffffff;text-decoration:none;padding:11px 15px;border-radius:12px;font-weight:800;">Open AO Live</a>
        </div>
        """
        if ao_live_url
        else ""
    )

    return f"""
<div style="margin:0;padding:8px;background:#0f1115;">
  <div style="max-width:680px;margin:0 auto;font-family:Arial,Helvetica,sans-serif;color:#f8fafc;line-height:1.5;">
    <div style="background:#181c23;border:1px solid #2b3240;border-radius:18px;padding:16px 14px;">
      <div style="font-size:30px;line-height:1.05;font-weight:900;letter-spacing:-0.04em;margin:0 0 8px;">AO Daily</div>
      <div style="font-size:13px;line-height:1.45;color:#98a2b3;">Seed check sent separately: {escape(seed_subject)}</div>

      {_relay_html(summary, outreach_digest)}
      {_duet_section_html()}

      {live_button}
    </div>
  </div>
</div>
""".strip()


def _daily_update_text(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})
    recommendation = _recommendation_ascii(summary, outreach_digest)
    seed_subject = _ascii_safe(seed_result.get("subject", "[AO Seed Check]"))
    ao_live_url = str(_ao_live_url() or "").strip()

    lines = [
        "AO Daily",
        "",
        f"Seed check sent separately: {seed_subject}",
        "",
        "Relay:",
        _ascii_safe(f"- money today: ${today.get('gross_usd', 0)} | payments: {today.get('payments_count', 0)}"),
        _ascii_safe(f"- money week: ${week.get('gross_usd', 0)} | month: ${month.get('gross_usd', 0)}"),
        _ascii_safe(f"- emails sent: {outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)}"),
        _ascii_safe(f"- replies: {outreach_digest.get('replies_today', 0)}"),
        _ascii_safe(f"- queue: {outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"),
        _ascii_safe(f"- best Relay move: {recommendation}"),
    ]

    lines.extend(_duet_section_text_lines())

    if ao_live_url:
        lines.extend(["", f"AO Live: {ao_live_url}"])

    return "\n".join(lines)


def _daily_update_subject(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    replies_today = _safe_int(outreach_digest.get("replies_today", 0))
    sent_today = _safe_int(outreach_digest.get("sent_today", 0))
    due_now = _safe_int(outreach_digest.get("due_now_count", 0))
    money_today = today.get("gross_usd", 0)

    duet = _duet_intel() if "_duet_intel" in globals() else {}
    duet_bits = ""
    if duet.get("status") == "ok":
        duet_bits = f" | duet hot {duet.get('hot_count', 0)}/{duet.get('active_threads', 0)}"

    return _ascii_safe(f"[AO Daily] Relay ${money_today} | replies {replies_today} | sent {sent_today} | queue {due_now}{duet_bits}")

# --- AO RELAY DUET SECTIONS END ---


# --- RELAY MAIL ONLY OVERRIDE START ---

def _relay_mail_live_url() -> str:
    for key in ("RELAY_LIVE_URL", "AO_LIVE_URL"):
        value = str(os.getenv(key, "") or "").strip().rstrip("/")
        if value:
            return value

    landing = str(os.getenv("LANDING_PAGE_URL", "") or "").strip().rstrip("/")
    if landing:
        return f"{landing}/ao_live_robinhood_desktop.html"

    return "https://relay.aolabs.io/admin.html"


def _relay_mail_state(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})

    payments_today = _safe_int(today.get("payments_count", 0))
    payments_week = _safe_int(week.get("payments_count", 0))
    replies_today = _safe_int(outreach_digest.get("replies_today", 0))
    sent_today = _safe_int(outreach_digest.get("sent_today", 0))
    daily_cap = _safe_int(outreach_digest.get("daily_send_cap", 0))
    due_now = _safe_int(outreach_digest.get("due_now_count", 0))
    send_window_open = bool(outreach_digest.get("send_window_is_open"))

    if payments_today > 0:
        return "Money landed today"
    if payments_week > 0:
        return "Money landed this week"
    if replies_today > 0:
        return "Reply signal"
    if daily_cap > 0 and sent_today >= daily_cap:
        return "Daily cap used"
    if sent_today > 0:
        return "Running"
    if due_now > 0 and send_window_open:
        return "Ready to send"
    if due_now > 0:
        return "Queued for next window"
    return "Off duty"


def _relay_mail_reassurance(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})

    payments_today = _safe_int(today.get("payments_count", 0))
    payments_week = _safe_int(week.get("payments_count", 0))
    replies_today = _safe_int(outreach_digest.get("replies_today", 0))
    sent_today = _safe_int(outreach_digest.get("sent_today", 0))
    daily_cap = _safe_int(outreach_digest.get("daily_send_cap", 0))
    due_now = _safe_int(outreach_digest.get("due_now_count", 0))

    if payments_today > 0:
        return "Payment signal exists today. The only high-value interruption is fulfillment."
    if payments_week > 0:
        return "Payment signal exists this week. Keep Relay stable and avoid changing the machine mid-signal."
    if replies_today > 0:
        return "A real reply exists. That is the closest thing to money before payment."
    if daily_cap > 0 and sent_today >= daily_cap:
        return "Today's safe send cap is used. More refreshing will not create replies; the next signal is a reply, payment, or the next run."
    if sent_today > 0:
        return "Clean sends happened today. Zero replies is not a verdict yet; judge after several clean send days."
    if due_now > 0:
        return "Clean leads are queued. The system has work available when the send window opens."
    return "No urgent Relay action is visible from these counters. Stay out of the loop unless replies, money, or deploy health changes."


def _relay_mail_next_move(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})

    replies_today = _safe_int(outreach_digest.get("replies_today", 0))
    sent_today = _safe_int(outreach_digest.get("sent_today", 0))
    due_now = _safe_int(outreach_digest.get("due_now_count", 0))
    payments_today = _safe_int(today.get("payments_count", 0))
    payments_week = _safe_int(week.get("payments_count", 0))

    if payments_today > 0 or payments_week > 0:
        return "Fulfill paid work. Do not tune outreach while money signal is active."
    if replies_today > 0:
        return "Answer real replies first. Do not scale until the objections are understood."
    if sent_today <= 0 and due_now > 0:
        return "Confirm the runner sends during the next window; leads are queued but no sends are logged today."
    if sent_today > 0 and due_now > 0:
        return "Keep volume flat. If replies stay at zero after 2-3 clean send days, improve targeting and opener clarity."
    if due_now <= 0:
        return "Add cleaner direct leads before judging the offer."
    return "Let the next send window run."


def _relay_mail_metric_html(label: str, value: str, note: str = "") -> str:
    note_html = f'<div style="font-size:12px;color:#99a3b3;margin-top:3px;">{escape(note)}</div>' if note else ""
    return f"""
      <div style="border:1px solid #2b3240;border-radius:14px;padding:12px 14px;margin:8px 0;background:#0f131a;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#99a3b3;font-weight:800;">{escape(label)}</div>
        <div style="font-size:24px;line-height:1.15;color:#f8fafc;font-weight:900;margin-top:4px;">{escape(value)}</div>
        {note_html}
      </div>
    """.strip()


def _relay_mail_metrics_html(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})

    rows = [
        ("Money today", f"${today.get('gross_usd', 0)}", f"{today.get('payments_count', 0)} payments"),
        ("Money week", f"${week.get('gross_usd', 0)}", f"month ${month.get('gross_usd', 0)}"),
        ("Replies", str(outreach_digest.get("replies_today", 0)), "today"),
        ("Sends", f"{outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)}", "safe daily cap"),
        ("Queue", str(outreach_digest.get("due_now_count", 0)), f"{outreach_digest.get('in_sequence_count', 0)} in sequence"),
    ]
    return "".join(_relay_mail_metric_html(label, value, note) for label, value, note in rows)


def _daily_update_html(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    state = _relay_mail_state(summary, outreach_digest)
    reassurance = _relay_mail_reassurance(summary, outreach_digest)
    next_move = _relay_mail_next_move(summary, outreach_digest)
    live_url = _relay_mail_live_url()

    live_button = (
        f"""
        <div style="margin:16px 0 0;">
          <a href="{escape(live_url)}" style="display:inline-block;background:#f8fafc;color:#0f1115;text-decoration:none;padding:11px 15px;border-radius:12px;font-weight:900;">Open Relay Admin</a>
        </div>
        """
        if live_url
        else ""
    )

    return f"""
<div style="margin:0;padding:8px;background:#0f1115;">
  <div style="max-width:680px;margin:0 auto;font-family:Arial,Helvetica,sans-serif;color:#f8fafc;line-height:1.5;">
    <div style="background:#181c23;border:1px solid #2b3240;border-radius:18px;padding:16px 14px;">
      <div style="font-size:12px;letter-spacing:0.12em;text-transform:uppercase;color:#99a3b3;font-weight:900;">Relay Mail</div>
      <div style="font-size:30px;line-height:1.05;font-weight:900;letter-spacing:-0.03em;margin:6px 0 8px;">{escape(state)}</div>
      <div style="font-size:14px;line-height:1.45;color:#d9e0ea;margin:0 0 12px;">{escape(reassurance)}</div>

      {_relay_mail_metrics_html(summary, outreach_digest)}

      <div style="border:1px solid #394150;border-radius:14px;padding:12px 14px;margin:12px 0 0;background:#111722;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#99a3b3;font-weight:800;">Next useful move</div>
        <div style="font-size:15px;line-height:1.45;color:#f8fafc;font-weight:700;margin-top:4px;">{escape(next_move)}</div>
      </div>

      {live_button}
    </div>
  </div>
</div>
""".strip()


def _daily_update_text(summary: dict[str, Any], outreach_digest: dict[str, Any], seed_result: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})
    live_url = _relay_mail_live_url()

    lines = [
        "Relay Mail",
        "",
        f"State: {_relay_mail_state(summary, outreach_digest)}",
        f"What this means: {_relay_mail_reassurance(summary, outreach_digest)}",
        "",
        _ascii_safe(f"Money today: ${today.get('gross_usd', 0)} from {today.get('payments_count', 0)} payments"),
        _ascii_safe(f"Money week: ${week.get('gross_usd', 0)} | month: ${month.get('gross_usd', 0)}"),
        _ascii_safe(f"Replies today: {outreach_digest.get('replies_today', 0)}"),
        _ascii_safe(f"Sends today: {outreach_digest.get('sent_today', 0)} / {outreach_digest.get('daily_send_cap', 0)}"),
        _ascii_safe(f"Queue: {outreach_digest.get('due_now_count', 0)} due now | {outreach_digest.get('in_sequence_count', 0)} in sequence"),
        "",
        _ascii_safe(f"Next useful move: {_relay_mail_next_move(summary, outreach_digest)}"),
    ]

    if live_url:
        lines.extend(["", f"Relay Admin: {live_url}"])

    return "\n".join(lines)


def _daily_update_subject(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    replies_today = _safe_int(outreach_digest.get("replies_today", 0))
    sent_today = _safe_int(outreach_digest.get("sent_today", 0))
    due_now = _safe_int(outreach_digest.get("due_now_count", 0))
    money_today = today.get("gross_usd", 0)
    state = _relay_mail_state(summary, outreach_digest)
    return _ascii_safe(f"[Relay Mail] {state} | ${money_today} | replies {replies_today} | sent {sent_today} | queue {due_now}")


def send_daily_money_summary() -> dict[str, Any]:
    summary = money_summary()
    outreach_digest = outreach_status()
    seed_result = {"status": "skipped", "reason": "relay_mail_is_the_delivery_check"}

    update_subject = _daily_update_subject(summary, outreach_digest)
    update_html = _daily_update_html(summary, outreach_digest, seed_result)
    update_text = _daily_update_text(summary, outreach_digest, seed_result)
    update_send_result = _send_smtp_email_from_seed_sender(
        subject=update_subject,
        text_body=update_text,
        html_body=update_html,
    )

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type="daily_ops_update_sent",
                prospect_external_id="ops",
                summary=update_subject,
                payload_json=json.dumps(
                    {
                        "mail_name": "Relay Mail",
                        "money": summary,
                        "outreach_digest": outreach_digest,
                        "seed_result": seed_result,
                        "update_send_result": update_send_result,
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

    return {
        "status": "ok",
        "mail_name": "Relay Mail",
        "seed_result": seed_result,
        "update_subject": update_subject,
        "update_send_result": update_send_result,
        "summary": summary,
        "outreach_digest": outreach_digest,
    }

# --- RELAY MAIL ONLY OVERRIDE END ---


# --- AO DIGEST OVERRIDE START ---

import urllib.error as _ao_digest_urlerror
import urllib.request as _ao_digest_urlrequest


def _ao_digest_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _ao_digest_money(value: Any) -> str:
    try:
        amount = float(value or 0)
    except Exception:
        amount = 0.0
    if amount.is_integer():
        return f"${int(amount)}"
    return f"${amount:.2f}"


def _ao_digest_relay_live_url() -> str:
    for key in ("RELAY_LIVE_URL", "AO_LIVE_URL"):
        value = str(os.getenv(key, "") or "").strip().rstrip("/")
        if value:
            return value
    return "https://relay.aolabs.io/admin.html"


def _ao_digest_ocean_url() -> str:
    return str(os.getenv("OCEAN_URL", "") or "").strip().rstrip("/") or "https://ocean.aolabs.io"


def _ao_digest_ocean_api_url() -> str:
    base = str(os.getenv("OCEAN_API_BASE_URL", "") or "").strip().rstrip("/") or _ao_digest_ocean_url()
    return f"{base}/api/radar"


def _ao_digest_event_count(event_types: list[str] | None = None, like_pattern: str | None = None) -> int:
    with _session() as session:
        query = select(func.count(AcquisitionEvent.id))
        if event_types:
            query = query.where(AcquisitionEvent.event_type.in_(event_types))
        if like_pattern:
            query = query.where(AcquisitionEvent.event_type.like(like_pattern))
        return _ao_digest_int(session.execute(query).scalar())


def _ao_digest_total_sends(outreach_digest: dict[str, Any]) -> int:
    existing = outreach_digest.get("total_sends_all_time")
    if existing is not None:
        return _ao_digest_int(existing)
    return _ao_digest_event_count(like_pattern="custom_outreach_sent_step_%")


def _ao_digest_total_replies(outreach_digest: dict[str, Any]) -> int:
    existing = outreach_digest.get("total_replies_all_time")
    if existing is not None:
        return _ao_digest_int(existing)
    return _ao_digest_event_count(["custom_outreach_reply_seen", "smartlead_reply"])


def _ao_digest_success_status() -> dict[str, Any]:
    try:
        status = relay_success_status()
        return status if isinstance(status, dict) else {"status": "error", "error": "empty_success_status"}
    except Exception as error:
        return {"status": "error", "error": str(error)}


def _ao_digest_nested(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    value = mapping.get(key) if isinstance(mapping, dict) else {}
    return value if isinstance(value, dict) else {}


def _ao_digest_operator_mode(
    summary: dict[str, Any],
    outreach_digest: dict[str, Any],
    success_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    success_status = success_status if isinstance(success_status, dict) else {}
    snapshot = _ao_digest_nested(success_status, "snapshot")
    money = _ao_digest_nested(snapshot, "money")
    intent = _ao_digest_nested(snapshot, "intent")
    outreach = _ao_digest_nested(snapshot, "outreach")

    bottleneck = str(success_status.get("bottleneck") or "").strip()
    payments = _ao_digest_int(money.get("payments") or summary.get("week", {}).get("payments_count"))
    replies = _ao_digest_int(outreach.get("replies") or outreach_digest.get("total_replies_all_time") or outreach_digest.get("replies_today"))
    auto_replies = _ao_digest_int(outreach.get("auto_replies") or outreach_digest.get("auto_replies_today"))
    unhandled_replies = _ao_digest_int(
        outreach.get("unhandled_replies")
        if outreach.get("unhandled_replies") is not None
        else max(replies - auto_replies - payments, 0)
    )
    checkout_clicks = _ao_digest_int(intent.get("checkout_clicks"))
    due = _ao_digest_int(outreach.get("due_now") or outreach_digest.get("direct_due_count") or outreach_digest.get("due_now_count"))
    cap_remaining = _ao_digest_int(outreach.get("cap_remaining") or outreach_digest.get("cap_remaining"))
    send_window_open = bool(outreach.get("send_window_is_open") or outreach_digest.get("send_window_is_open"))
    active_needs_sample = bool(
        outreach.get("active_experiment_needs_sample")
        or outreach_digest.get("active_experiment_needs_sample")
    )
    active_due = _ao_digest_int(
        outreach.get("active_experiment_new_due_count")
        or outreach_digest.get("active_experiment_new_due_count")
    )
    next_window = str(
        outreach.get("send_window_next_open_local")
        or outreach_digest.get("send_window_next_open_local")
        or ""
    ).strip()

    urgent = {
        "infrastructure_blocked",
        "paid_fulfillment",
        "checkout_to_payment",
        "reply_to_payment",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
        "outbound_window_underfilled",
    }
    followup = {"messy_notes_to_payment", "sample_to_notes"}

    if bottleneck in urgent or unhandled_replies > 0 or checkout_clicks > payments:
        return {
            "mode": "attention_required",
            "label": "Attention needed",
            "do_not_interrupt_user": False,
            "reason": success_status.get("next_action") or bottleneck or "buyer signal needs handling",
            "next_window": next_window,
        }
    if bottleneck in followup:
        return {
            "mode": "autonomous_followup_due",
            "label": "Stay out",
            "do_not_interrupt_user": True,
            "reason": success_status.get("next_action") or "Relay has a follow-up queued.",
            "next_window": next_window,
        }

    ready_due = active_due if active_needs_sample else due
    if ready_due > 0 and cap_remaining > 0:
        if send_window_open:
            return {
                "mode": "autonomous_sending_now",
                "label": "Stay out",
                "do_not_interrupt_user": True,
                "reason": "Send window is open and Relay has queued leads.",
                "next_window": next_window,
            }
        return {
            "mode": "out_of_loop_waiting",
            "label": "Stay out",
            "do_not_interrupt_user": True,
            "reason": "Queued leads are ready for the next send window.",
            "next_window": next_window,
        }

    return {
        "mode": "out_of_loop_monitoring",
        "label": "Stay out",
        "do_not_interrupt_user": True,
        "reason": success_status.get("next_action") or "No immediate human action is useful.",
        "next_window": next_window,
    }


def _ao_digest_operator_note(operator: dict[str, Any]) -> str:
    reason = str(operator.get("reason") or "").strip()
    next_window = str(operator.get("next_window") or "").strip()
    if next_window and operator.get("do_not_interrupt_user"):
        return f"{reason} Next window: {next_window}"
    return reason


def _ao_digest_ceil_div(numerator: int, denominator: int) -> int:
    if numerator <= 0 or denominator <= 0:
        return 0
    return (numerator + denominator - 1) // denominator


def _ao_digest_parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _ao_digest_next_window_audit_at(outreach_digest: dict[str, Any], operator: dict[str, Any]) -> str:
    reason = str(outreach_digest.get("send_window_reason") or "").strip()
    end_local = str(outreach_digest.get("send_window_end_local") or "").strip()
    if reason == "open" and end_local:
        return end_local

    next_dt = _ao_digest_parse_datetime(
        operator.get("next_window")
        or outreach_digest.get("send_window_next_open_local")
    )
    start_dt = _ao_digest_parse_datetime(outreach_digest.get("send_window_start_local"))
    end_dt = _ao_digest_parse_datetime(outreach_digest.get("send_window_end_local"))
    if next_dt is not None and start_dt is not None and end_dt is not None:
        duration = end_dt - start_dt
        if duration.total_seconds() > 0:
            return (next_dt + duration).isoformat()
    return str(outreach_digest.get("send_window_end_local") or outreach_digest.get("send_window_next_open_local") or "").strip()


def _ao_digest_window_execution_state(
    *,
    outreach_digest: dict[str, Any],
    active_remaining: int,
    active_due: int,
    expected_next_window_sends: int,
) -> str:
    reason = str(outreach_digest.get("send_window_reason") or "").strip()
    sent_today = _ao_digest_int(outreach_digest.get("sent_today"))
    cap_remaining = _ao_digest_int(outreach_digest.get("cap_remaining"))
    if active_remaining <= 0:
        return "sample_complete"
    if expected_next_window_sends <= 0:
        return "blocked"
    if reason == "open":
        return "window_open"
    if reason == "after_window" and active_due > 0 and cap_remaining > 0 and sent_today <= 0:
        return "window_missed"
    if reason == "after_window" and active_due > 0 and cap_remaining > 0:
        return "window_underfilled"
    if reason == "after_window":
        return "window_passed"
    return "waiting_for_window"


def _ao_digest_launch_readiness(
    summary: dict[str, Any],
    outreach_digest: dict[str, Any],
    success_status: dict[str, Any] | None = None,
    operator: dict[str, Any] | None = None,
) -> dict[str, Any]:
    success_status = success_status if isinstance(success_status, dict) else {}
    snapshot = _ao_digest_nested(success_status, "snapshot")
    performance = _ao_digest_nested(snapshot, "performance")
    active_signal = _ao_digest_nested(performance, "active_experiment_signal")
    operator = operator or _ao_digest_operator_mode(summary, outreach_digest, success_status)

    active_sends = _ao_digest_int(
        outreach_digest.get("active_experiment_sample_sends")
        or outreach_digest.get("active_experiment_sends")
    )
    active_target = _ao_digest_int(outreach_digest.get("active_experiment_sample_target"))
    active_due = _ao_digest_int(outreach_digest.get("active_experiment_new_due_count"))
    active_remaining = max(active_target - active_sends, 0) if active_target else 0
    cap_remaining = _ao_digest_int(outreach_digest.get("cap_remaining"))
    daily_cap = _ao_digest_int(outreach_digest.get("effective_daily_cap") or outreach_digest.get("daily_send_cap"))
    capacity = max(min(cap_remaining, daily_cap or cap_remaining), 0)
    future_capacity = max(daily_cap, capacity)
    estimated_future_window_capacity = (
        min(active_due, future_capacity)
        if active_remaining > 0 and active_due > 0 and future_capacity > 0
        else 0
    )
    windows_remaining = _ao_digest_ceil_div(active_remaining, estimated_future_window_capacity)
    expected_next_window_sends = (
        min(active_remaining, active_due, capacity)
        if active_remaining > 0 and active_due > 0 and capacity > 0
        else 0
    )
    expected_sends_after_next_window = min(active_sends + expected_next_window_sends, active_target) if active_target else active_sends
    expected_progress_after_next_window = (
        f"{expected_sends_after_next_window}/{active_target}"
        if active_target
        else ""
    )
    if active_remaining > 0 and expected_next_window_sends > 0:
        next_window_success_criterion = (
            f"send {expected_next_window_sends} active leads and move progress "
            f"from {active_sends}/{active_target} to {expected_progress_after_next_window}"
        )
    elif active_remaining > 0:
        next_window_success_criterion = "make queued active leads and send capacity available"
    else:
        next_window_success_criterion = "review the completed active sample and keep or rotate one variable"
    next_window_audit_at = _ao_digest_next_window_audit_at(outreach_digest, operator)
    window_execution_state = _ao_digest_window_execution_state(
        outreach_digest=outreach_digest,
        active_remaining=active_remaining,
        active_due=active_due,
        expected_next_window_sends=expected_next_window_sends,
    )
    if expected_next_window_sends > 0:
        window_execution_failure_condition = (
            f"after audit time, interrupt if fewer than {expected_next_window_sends} active sends completed "
            "or the window closes with queued active leads and unused capacity"
        )
    else:
        window_execution_failure_condition = "interrupt if the loop cannot create queued active leads or send capacity"
    next_window = str(
        operator.get("next_window")
        or outreach_digest.get("send_window_next_open_local")
        or ""
    ).strip()
    active_replies = _ao_digest_int(active_signal.get("replies"))
    active_payments = _ao_digest_int(active_signal.get("payments"))
    outreach = _ao_digest_nested(snapshot, "outreach")
    replies = _ao_digest_int(outreach.get("replies"))
    auto_replies = _ao_digest_int(outreach.get("auto_replies"))
    payments = _ao_digest_int(_ao_digest_nested(snapshot, "money").get("payments"))
    unhandled_replies = _ao_digest_int(
        outreach.get("unhandled_replies")
        if outreach.get("unhandled_replies") is not None
        else max(replies - auto_replies - payments, 0)
    )

    blockers: list[str] = []
    execution_blocker_states = {
        "infrastructure_blocked",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
        "outbound_window_underfilled",
    }
    bottleneck = str(success_status.get("bottleneck") or "").strip()
    if bottleneck in execution_blocker_states:
        blockers.append(str(success_status.get("next_action") or bottleneck))
    money_loop = outreach_digest.get("money_loop") if isinstance(outreach_digest.get("money_loop"), dict) else {}
    loop_status = str(money_loop.get("status") or "").strip()
    if loop_status in {"disabled", "error", "stuck", "late"}:
        blockers.append(f"money loop is {loop_status}")
    if not str(settings.packet_checkout_url or "").strip():
        blockers.append("entry checkout link is not configured")
    if active_target <= 0:
        blockers.append("active experiment sample target is missing")
    if active_remaining > 0 and active_due <= 0:
        blockers.append("active experiment needs more queued first-touch leads")
    if active_remaining > 0 and windows_remaining <= 0:
        blockers.append("active experiment has no estimated send window capacity")
    if active_remaining > 0 and not next_window:
        blockers.append("next send window is not known")

    if active_payments > 0:
        interrupt_rule = "interrupt for fulfillment and keep the winning lane stable"
    elif unhandled_replies > 0:
        interrupt_rule = "interrupt to close unhandled replies through the paid next step"
    else:
        interrupt_rule = "do not interrupt unless unhandled replies, checkout/payment signal, or system health changes"

    proof_target = (
        f"collect {active_remaining} more active-variant sends"
        if active_remaining > 0
        else "judge the completed active sample"
    )
    success_metric = (
        "first real reply, checkout/payment signal, or completed active sample"
        if active_remaining > 0
        else "active replies/payments decide whether to keep stable or rotate one variable"
    )

    return {
        "ready": not blockers,
        "blockers": blockers,
        "proof_target": proof_target,
        "success_metric": success_metric,
        "interrupt_rule": interrupt_rule,
        "active_experiment_progress": f"{active_sends}/{active_target}" if active_target else "",
        "expected_next_window_sends": expected_next_window_sends,
        "expected_progress_after_next_window": expected_progress_after_next_window,
        "next_window_success_criterion": next_window_success_criterion,
        "next_window_audit_at": next_window_audit_at,
        "window_execution_state": window_execution_state,
        "window_execution_failure_condition": window_execution_failure_condition,
        "window_execution_contract": {
            "state": window_execution_state,
            "expected_sends": expected_next_window_sends,
            "expected_progress": expected_progress_after_next_window,
            "audit_at": next_window_audit_at,
            "success_criterion": next_window_success_criterion,
            "failure_condition": window_execution_failure_condition,
        },
        "estimated_windows_remaining": windows_remaining,
        "next_autonomous_window": next_window,
        "review_rule": "do not judge the offer until the active sample is complete or real buyer signal appears",
    }


def _ao_digest_fetch_json(url: str, timeout_seconds: int = 8) -> dict[str, Any]:
    request = _ao_digest_urlrequest.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "AO Digest/1.0",
        },
    )
    with _ao_digest_urlrequest.urlopen(request, timeout=timeout_seconds) as response:
        raw = response.read(2_000_000)
    return json.loads(raw.decode("utf-8", errors="replace"))


def _ao_digest_static_ocean_items() -> list[dict[str, Any]]:
    return [
        {
            "type": "Field signal",
            "source": "Disney Imagineering",
            "title": "Watch creative technology, animatronics, show systems, robotics, and R&D roles.",
            "url": "https://jobs.disneycareers.com/search-jobs?k=imagineering%20research%20development%20creative%20technology",
            "why": "Use role language to decide what proof to build: mechanisms, interaction, reliability, and audience experience.",
        },
        {
            "type": "Learning lane",
            "source": "Disney Research Studios",
            "title": "Study how research becomes tools, characters, environments, effects, and physical experiences.",
            "url": "https://studios.disneyresearch.com/",
            "why": "The goal is not one narrow topic. The goal is becoming strong at turning technical depth into memorable experiences.",
        },
        {
            "type": "Build idea",
            "source": "Ocean",
            "title": "Make one small physical or interactive proof this week.",
            "url": _ao_digest_ocean_url(),
            "why": "A measured prototype, a short demo, or a clear technical note moves the career forward more than rereading the whole plan.",
        },
    ]


def _ao_digest_ocean_digest(limit: int = 5) -> dict[str, Any]:
    try:
        radar = _ao_digest_fetch_json(_ao_digest_ocean_api_url())
        items = radar.get("items") if isinstance(radar, dict) else []
        clean_items = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            clean_items.append(
                {
                    "type": str(item.get("type") or "Signal").strip(),
                    "source": str(item.get("source") or "Ocean").strip(),
                    "title": str(item.get("title") or "Untitled signal").strip(),
                    "url": str(item.get("url") or "").strip(),
                    "why": str(item.get("why") or item.get("summary") or "").strip(),
                    "date": str(item.get("date") or "").strip(),
                    "location": str(item.get("location") or "").strip(),
                }
            )
        return {
            "status": "ok",
            "updated_at": radar.get("updatedAt"),
            "items": clean_items[:limit] if clean_items else _ao_digest_static_ocean_items(),
            "source_status": radar.get("sourceStatus") if isinstance(radar.get("sourceStatus"), list) else [],
            "error": radar.get("error"),
        }
    except (_ao_digest_urlerror.URLError, TimeoutError, json.JSONDecodeError, Exception) as error:
        return {
            "status": "fallback",
            "updated_at": None,
            "items": _ao_digest_static_ocean_items(),
            "source_status": [],
            "error": str(error),
        }


def _ao_digest_relay_state(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    today = summary.get("today", {})
    week = summary.get("week", {})
    replies_today = _ao_digest_int(outreach_digest.get("replies_today"))
    sent_today = _ao_digest_int(outreach_digest.get("sent_today"))
    payments_today = _ao_digest_int(today.get("payments_count"))
    payments_week = _ao_digest_int(week.get("payments_count"))
    due = _ao_digest_int(outreach_digest.get("direct_due_count", outreach_digest.get("due_now_count")))
    cap_remaining = _ao_digest_int(outreach_digest.get("cap_remaining"))

    if payments_today > 0:
        return "Money landed today"
    if payments_week > 0:
        return "Money landed this week"
    if replies_today > 0:
        return "Reply signal exists"
    if sent_today > 0:
        return "Outreach is running"
    if due > 0 and cap_remaining > 0:
        return "Leads are ready"
    return "Keep the loop stable"


def _ao_digest_relay_move(summary: dict[str, Any], outreach_digest: dict[str, Any]) -> str:
    explicit = str(outreach_digest.get("next_money_move") or "").strip()
    if explicit:
        return explicit

    replies_today = _ao_digest_int(outreach_digest.get("replies_today"))
    due = _ao_digest_int(outreach_digest.get("direct_due_count", outreach_digest.get("due_now_count")))
    sent_today = _ao_digest_int(outreach_digest.get("sent_today"))
    daily_cap = _ao_digest_int(outreach_digest.get("daily_send_cap"))

    if replies_today > 0:
        return "Handle replies first. Real humans are the closest money."
    if daily_cap > 0 and sent_today >= daily_cap:
        return "Daily cap is used. Wait for replies or the next send window."
    if due > 0:
        return "Let direct leads send during the next window. Do not chase generic inboxes."
    return "Refill direct decision-maker leads before increasing volume."


def _ao_digest_life_note() -> dict[str, str]:
    notes = [
        {
            "title": "Stress is information, not a command.",
            "body": "Someone having an opinion does not mean you did something wrong. Feel the stress, slow down, and keep the next action clean.",
        },
        {
            "title": "Getting rich still uses ordinary reps.",
            "body": "The work is not to feel certain first. The work is to keep sending, learning, building, and answering real signals when they appear.",
        },
        {
            "title": "You can be uncomfortable and still be effective.",
            "body": "Let the body react without letting it run the day. Other people can feel however they feel; your job is clarity and consistency.",
        },
        {
            "title": "Pressure tolerance is a skill.",
            "body": "Every day you practice staying steady under uncertainty, you make it easier to sell, build, ask, date, and lead without collapsing into defense.",
        },
    ]
    index = datetime.utcnow().timetuple().tm_yday % len(notes)
    return notes[index]


def _ao_digest_metric_html(label: str, value: str, note: str = "") -> str:
    note_html = f'<div style="font-size:12px;color:#64748b;margin-top:3px;">{escape(note)}</div>' if note else ""
    return f"""
      <div style="border:1px solid #d8dee8;border-radius:12px;padding:12px 14px;background:#ffffff;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;font-weight:800;">{escape(label)}</div>
        <div style="font-size:24px;line-height:1.15;color:#101827;font-weight:900;margin-top:4px;">{escape(value)}</div>
        {note_html}
      </div>
    """.strip()


def _ao_digest_ocean_items_html(ocean_digest: dict[str, Any]) -> str:
    items = ocean_digest.get("items") if isinstance(ocean_digest.get("items"), list) else []
    rows = []
    for item in items[:5]:
        title = str(item.get("title") or "Untitled signal")
        source = str(item.get("source") or "Ocean")
        item_type = str(item.get("type") or "Signal")
        url = str(item.get("url") or "")
        why = str(item.get("why") or "")
        link = (
            f'<a href="{escape(url)}" style="color:#2458d3;text-decoration:none;font-weight:800;">{escape(title)}</a>'
            if url
            else f'<strong style="color:#101827;">{escape(title)}</strong>'
        )
        rows.append(
            f"""
            <div style="border-top:1px solid #e5e7eb;padding:12px 0;">
              <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;font-weight:800;">{escape(item_type)} / {escape(source)}</div>
              <div style="font-size:15px;line-height:1.35;margin:3px 0;">{link}</div>
              <div style="font-size:13px;line-height:1.45;color:#475569;">{escape(why)}</div>
            </div>
            """.strip()
        )
    return "".join(rows) or '<div style="font-size:13px;color:#64748b;">No Ocean items available today.</div>'


def _daily_update_subject(
    summary: dict[str, Any],
    outreach_digest: dict[str, Any],
    success_status: dict[str, Any] | None = None,
) -> str:
    today = summary.get("today", {})
    money_today = _ao_digest_money(today.get("gross_usd", 0))
    sent_today = _ao_digest_int(outreach_digest.get("sent_today"))
    replies_today = _ao_digest_int(outreach_digest.get("replies_today"))
    total_sent = _ao_digest_total_sends(outreach_digest)
    total_replies = _ao_digest_total_replies(outreach_digest)
    operator = _ao_digest_operator_mode(summary, outreach_digest, success_status)
    return _ascii_safe(
        f"[AO Digest] {operator['label']} | Relay {money_today} | sent {sent_today}/{total_sent} | replies {replies_today}/{total_replies}"
    )


def _daily_update_html(
    summary: dict[str, Any],
    outreach_digest: dict[str, Any],
    seed_result: dict[str, Any],
    ocean_digest: dict[str, Any] | None = None,
    success_status: dict[str, Any] | None = None,
) -> str:
    ocean_digest = ocean_digest or _ao_digest_ocean_digest()
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})
    target = outreach_digest.get("money_target") if isinstance(outreach_digest.get("money_target"), dict) else {}
    life = _ao_digest_life_note()
    total_sent = _ao_digest_total_sends(outreach_digest)
    total_replies = _ao_digest_total_replies(outreach_digest)
    relay_url = _ao_digest_relay_live_url()
    ocean_url = _ao_digest_ocean_url()
    relay_state = _ao_digest_relay_state(summary, outreach_digest)
    relay_move = _ao_digest_relay_move(summary, outreach_digest)
    operator = _ao_digest_operator_mode(summary, outreach_digest, success_status)
    operator_note = _ao_digest_operator_note(operator)
    readiness = _ao_digest_launch_readiness(summary, outreach_digest, success_status, operator)
    readiness_note = (
        "Ready: no blockers"
        if readiness.get("ready")
        else "Blocked: " + "; ".join(readiness.get("blockers") or [])
    )

    return f"""
<div style="margin:0;padding:12px;background:#f6f7f9;">
  <div style="max-width:720px;margin:0 auto;font-family:Arial,Helvetica,sans-serif;color:#101827;line-height:1.5;">
    <div style="background:#ffffff;border:1px solid #d8dee8;border-radius:18px;padding:18px 16px;margin-bottom:12px;">
      <div style="font-size:12px;letter-spacing:0.12em;text-transform:uppercase;color:#2458d3;font-weight:900;">AO Digest</div>
      <div style="font-size:30px;line-height:1.05;font-weight:900;letter-spacing:-0.03em;margin:6px 0;">{escape(operator["label"])}</div>
      <div style="font-size:14px;color:#475569;">{escape(operator_note)}</div>
    </div>

    <div style="background:#ffffff;border:1px solid #d8dee8;border-radius:18px;padding:16px;margin-bottom:12px;">
      <div style="font-size:18px;font-weight:900;margin-bottom:10px;">Relay</div>
      <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;">
        {_ao_digest_metric_html("Operator", str(operator["label"]), "interrupt only for replies, payments, or health failures")}
        {_ao_digest_metric_html("State", relay_state, str(operator.get("mode") or ""))}
        {_ao_digest_metric_html("Money today", _ao_digest_money(today.get("gross_usd")), f"{today.get('payments_count', 0)} payments")}
        {_ao_digest_metric_html("Money week", _ao_digest_money(week.get("gross_usd")), f"month {_ao_digest_money(month.get('gross_usd'))}")}
        {_ao_digest_metric_html("Emails sent", str(total_sent), f"today {outreach_digest.get('sent_today', 0)} / cap {outreach_digest.get('daily_send_cap', 0)}")}
        {_ao_digest_metric_html("Replies received", str(total_replies), f"today {outreach_digest.get('replies_today', 0)}")}
        {_ao_digest_metric_html("Direct leads due", str(outreach_digest.get("direct_due_count", outreach_digest.get("due_now_count", 0))), f"{outreach_digest.get('generic_paused_count', 0)} generic paused")}
        {_ao_digest_metric_html("Weekly target", _ao_digest_money(target.get("weekly_target_usd", 100)), f"{target.get('paid_tests_needed_weekly', 3)} paid tests needed")}
      </div>
      <div style="border:1px solid #d8dee8;border-radius:12px;padding:12px 14px;margin-top:12px;background:#f8fafc;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;font-weight:800;">Next money move</div>
        <div style="font-size:15px;line-height:1.45;font-weight:800;color:#101827;margin-top:4px;">{escape(relay_move)}</div>
      </div>
      <div style="border:1px solid #d8dee8;border-radius:12px;padding:12px 14px;margin-top:12px;background:#f8fafc;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;font-weight:800;">Launch contract</div>
        <div style="font-size:15px;line-height:1.45;font-weight:800;color:#101827;margin-top:4px;">{escape(str(readiness.get("proof_target") or ""))}</div>
        <div style="font-size:13px;line-height:1.45;color:#475569;margin-top:5px;">{escape(readiness_note)}</div>
        <div style="font-size:13px;line-height:1.45;color:#475569;margin-top:5px;">{escape(str(readiness.get("interrupt_rule") or ""))}</div>
      </div>
      <div style="margin-top:14px;">
        <a href="{escape(relay_url)}" style="display:inline-block;background:#101827;color:#ffffff;text-decoration:none;padding:10px 13px;border-radius:10px;font-weight:900;margin-right:8px;">Open Relay Admin</a>
      </div>
    </div>

    <div style="background:#ffffff;border:1px solid #d8dee8;border-radius:18px;padding:16px;margin-bottom:12px;">
      <div style="font-size:18px;font-weight:900;margin-bottom:4px;">Ocean</div>
      <div style="font-size:14px;color:#475569;margin-bottom:6px;">Learn broadly. Build evidence. Track creative R&D, Disney Imagineering, labs, robotics, interaction, fabrication, tools, and anything worth becoming good for.</div>
      {_ao_digest_ocean_items_html(ocean_digest)}
      <div style="border:1px solid #bfdbfe;border-radius:12px;padding:12px 14px;margin-top:10px;background:#f8fbff;">
        <div style="font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:#2458d3;font-weight:800;">Today&apos;s build move</div>
        <div style="font-size:14px;line-height:1.45;color:#172033;margin-top:4px;">Pick one signal above. Write three bullets: what it teaches, what proof it asks for, and one tiny thing you can prototype or study today.</div>
      </div>
      <div style="margin-top:14px;">
        <a href="{escape(ocean_url)}" style="display:inline-block;background:#2458d3;color:#ffffff;text-decoration:none;padding:10px 13px;border-radius:10px;font-weight:900;">Open Ocean</a>
      </div>
    </div>

    <div style="background:#ffffff;border:1px solid #d8dee8;border-radius:18px;padding:16px;">
      <div style="font-size:18px;font-weight:900;margin-bottom:6px;">Life / pressure</div>
      <div style="font-size:16px;font-weight:900;color:#101827;">{escape(life["title"])}</div>
      <div style="font-size:14px;line-height:1.5;color:#475569;margin-top:4px;">{escape(life["body"])}</div>
    </div>
  </div>
</div>
""".strip()


def _daily_update_text(
    summary: dict[str, Any],
    outreach_digest: dict[str, Any],
    seed_result: dict[str, Any],
    ocean_digest: dict[str, Any] | None = None,
    success_status: dict[str, Any] | None = None,
) -> str:
    ocean_digest = ocean_digest or _ao_digest_ocean_digest()
    today = summary.get("today", {})
    week = summary.get("week", {})
    month = summary.get("month", {})
    target = outreach_digest.get("money_target") if isinstance(outreach_digest.get("money_target"), dict) else {}
    life = _ao_digest_life_note()
    total_sent = _ao_digest_total_sends(outreach_digest)
    total_replies = _ao_digest_total_replies(outreach_digest)
    items = ocean_digest.get("items") if isinstance(ocean_digest.get("items"), list) else []
    operator = _ao_digest_operator_mode(summary, outreach_digest, success_status)
    readiness = _ao_digest_launch_readiness(summary, outreach_digest, success_status, operator)
    readiness_note = (
        "ready: no blockers"
        if readiness.get("ready")
        else "blocked: " + "; ".join(readiness.get("blockers") or [])
    )

    lines = [
        "AO Digest",
        "",
        "Relay",
        _ascii_safe(f"- operator: {operator['label']} ({_ao_digest_operator_note(operator)})"),
        _ascii_safe(f"- launch readiness: {readiness_note}"),
        _ascii_safe(f"- proof target: {readiness.get('proof_target', '')}"),
        _ascii_safe(f"- success metric: {readiness.get('success_metric', '')}"),
        _ascii_safe(f"- interrupt rule: {readiness.get('interrupt_rule', '')}"),
        f"- state: {_ao_digest_relay_state(summary, outreach_digest)}",
        _ascii_safe(f"- money today: {_ao_digest_money(today.get('gross_usd'))} from {today.get('payments_count', 0)} payments"),
        _ascii_safe(f"- money week: {_ao_digest_money(week.get('gross_usd'))} | month: {_ao_digest_money(month.get('gross_usd'))}"),
        _ascii_safe(f"- emails sent: {total_sent} total | {outreach_digest.get('sent_today', 0)} today / cap {outreach_digest.get('daily_send_cap', 0)}"),
        _ascii_safe(f"- replies received: {total_replies} total | {outreach_digest.get('replies_today', 0)} today"),
        _ascii_safe(f"- direct leads due: {outreach_digest.get('direct_due_count', outreach_digest.get('due_now_count', 0))} | generic paused: {outreach_digest.get('generic_paused_count', 0)}"),
        _ascii_safe(f"- weekly target: {_ao_digest_money(target.get('weekly_target_usd', 100))} | paid tests needed: {target.get('paid_tests_needed_weekly', 3)}"),
        _ascii_safe(f"- next money move: {_ao_digest_relay_move(summary, outreach_digest)}"),
        "",
        f"Relay Admin: {_ao_digest_relay_live_url()}",
        "",
        "Ocean",
        "Learn broadly. Build evidence. Track creative R&D, Disney Imagineering, labs, robotics, interaction, fabrication, tools, and anything worth becoming good for.",
    ]

    for item in items[:5]:
        lines.extend(
            [
                "",
                _ascii_safe(f"- {item.get('type', 'Signal')} / {item.get('source', 'Ocean')}: {item.get('title', 'Untitled signal')}"),
                _ascii_safe(f"  why: {item.get('why', '')}"),
            ]
        )
        if item.get("url"):
            lines.append(_ascii_safe(f"  link: {item.get('url')}"))

    lines.extend(
        [
            "",
            "Today's build move: Pick one Ocean signal. Write three bullets: what it teaches, what proof it asks for, and one tiny thing you can prototype or study today.",
            f"Ocean: {_ao_digest_ocean_url()}",
            "",
            "Life / pressure",
            _ascii_safe(f"- {life['title']} {life['body']}"),
        ]
    )

    return "\n".join(lines)


def send_daily_money_summary() -> dict[str, Any]:
    summary = money_summary()
    outreach_digest = outreach_status()
    ocean_digest = _ao_digest_ocean_digest()
    success_status = _ao_digest_success_status()
    operator_mode = _ao_digest_operator_mode(summary, outreach_digest, success_status)
    launch_readiness = _ao_digest_launch_readiness(summary, outreach_digest, success_status, operator_mode)
    seed_result = {"status": "skipped", "reason": "ao_digest_replaces_seed_check"}

    update_subject = _daily_update_subject(summary, outreach_digest, success_status)
    update_html = _daily_update_html(summary, outreach_digest, seed_result, ocean_digest, success_status)
    update_text = _daily_update_text(summary, outreach_digest, seed_result, ocean_digest, success_status)
    update_send_result = _send_smtp_email_from_seed_sender(
        subject=update_subject,
        text_body=update_text,
        html_body=update_html,
    )

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type="daily_ops_update_sent",
                prospect_external_id="ops",
                summary=update_subject,
                payload_json=json.dumps(
                    {
                        "mail_name": "AO Digest",
                        "money": summary,
                        "outreach_digest": outreach_digest,
                        "ocean_digest": ocean_digest,
                        "relay_success": success_status,
                        "operator_mode": operator_mode,
                        "launch_readiness": launch_readiness,
                        "seed_result": seed_result,
                        "update_send_result": update_send_result,
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

    return {
        "status": "ok",
        "mail_name": "AO Digest",
        "seed_result": seed_result,
        "update_subject": update_subject,
        "update_send_result": update_send_result,
        "summary": summary,
        "outreach_digest": outreach_digest,
        "ocean_digest": ocean_digest,
        "relay_success": success_status,
        "operator_mode": operator_mode,
        "launch_readiness": launch_readiness,
    }

# --- AO DIGEST OVERRIDE END ---

