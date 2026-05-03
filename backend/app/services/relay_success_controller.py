from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead
from app.services.post_purchase_autopilot import (
    run_inbound_conversion_sweep,
    run_paid_intake_reminder_sweep,
    run_post_delivery_upsell_sweep,
)
from app.services.relay_performance import relay_performance_status, run_weekly_performance_review


SUCCESS_TICK_EVENT = "relay_success_control_tick"


def _session() -> Session:
    return SessionLocal()


def _now() -> datetime:
    return datetime.utcnow()


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _internal_emails() -> set[str]:
    configured = os.getenv("RELAY_INTERNAL_EMAILS", "pham.alann@gmail.com").split(",")
    return {email.strip().lower() for email in configured if email.strip()}


def _is_internal_email(email: str | None) -> bool:
    return (email or "").strip().lower() in _internal_emails()


def _internal_session_ids(session: Session, *, since: datetime) -> set[str]:
    return {
        session_id
        for session_id in session.execute(
            select(RelayIntentLead.session_id)
            .where(RelayIntentLead.created_at >= since)
            .where(RelayIntentLead.email.in_(_internal_emails()))
        ).scalars().all()
        if session_id
    }


def _event_count(session: Session, event_type: str, *, since: datetime, like: bool = False) -> int:
    stmt = select(func.count(AcquisitionEvent.id))
    stmt = stmt.where(AcquisitionEvent.event_type.like(event_type) if like else AcquisitionEvent.event_type == event_type)
    stmt = stmt.where(AcquisitionEvent.created_at >= since)
    return int(session.execute(stmt).scalar() or 0)


def _intent_count(session: Session, event_type: str, *, since: datetime, exclude_sessions: set[str] | None = None) -> int:
    stmt = (
        select(func.count(RelayIntentEvent.id))
        .where(RelayIntentEvent.event_type == event_type)
        .where(RelayIntentEvent.created_at >= since)
    )
    if exclude_sessions:
        stmt = stmt.where(RelayIntentEvent.session_id.not_in(exclude_sessions))
    return int(
        session.execute(stmt).scalar()
        or 0
    )


def _lead_count(session: Session, source_term: str | None, *, since: datetime) -> int:
    stmt = (
        select(func.count(RelayIntentLead.id))
        .where(RelayIntentLead.created_at >= since)
        .where(RelayIntentLead.email.not_in(_internal_emails()))
    )
    if source_term:
        stmt = stmt.where(RelayIntentLead.source.ilike(f"%{source_term}%"))
    return int(session.execute(stmt).scalar() or 0)


def _stripe_email(payload: dict[str, Any]) -> str:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    return str(
        payload.get("customer_details", {}).get("email")
        or payload.get("customer_email")
        or payload.get("email")
        or raw_object.get("customer_details", {}).get("email")
        or raw_object.get("customer_email")
        or ""
    ).strip().lower()


def _stripe_amount_cents(payload: dict[str, Any]) -> int:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    try:
        return int(payload.get("amount_total") or raw_object.get("amount_total") or 0)
    except Exception:
        return 0


def _paid_for_email(session: Session, email: str) -> bool:
    email = (email or "").strip().lower()
    if not email or _is_internal_email(email):
        return False
    prospect = session.execute(
        select(AcquisitionProspect)
        .where(AcquisitionProspect.contact_email == email)
        .where(AcquisitionProspect.stripe_status == "paid")
        .limit(1)
    ).scalar_one_or_none()
    if prospect is not None:
        return True

    events = session.execute(
        select(AcquisitionEvent.payload_json)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(100)
    ).scalars().all()
    for raw in events:
        if email in json.dumps(_safe_json(raw), ensure_ascii=False).lower():
            return True
    return False


def _money_metrics(session: Session, *, since: datetime) -> dict[str, Any]:
    events = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .where(AcquisitionEvent.created_at >= since)
        .order_by(AcquisitionEvent.created_at.desc())
    ).scalars().all()

    payments = 0
    gross_cents = 0
    for event in events:
        payload = _safe_json(event.payload_json)
        if _is_internal_email(_stripe_email(payload)):
            continue
        payments += 1
        gross_cents += _stripe_amount_cents(payload)

    return {
        "payments": payments,
        "gross_cents": gross_cents,
        "gross_usd": round(gross_cents / 100.0, 2),
    }


def _due_followup_counts(session: Session, *, now: datetime) -> dict[str, int]:
    messy_cutoff = now - timedelta(hours=int(os.getenv("RELAY_MESSY_NOTES_FOLLOWUP_HOURS", "2") or "2"))
    messy_second_cutoff = now - timedelta(hours=int(os.getenv("RELAY_MESSY_NOTES_SECOND_FOLLOWUP_HOURS", "24") or "24"))
    sample_cutoff = now - timedelta(hours=int(os.getenv("RELAY_SAMPLE_FOLLOWUP_HOURS", "24") or "24"))
    sample_second_cutoff = now - timedelta(hours=int(os.getenv("RELAY_SAMPLE_SECOND_FOLLOWUP_HOURS", "72") or "72"))
    checkout_cutoff = now - timedelta(hours=int(os.getenv("RELAY_CHECKOUT_FOLLOWUP_HOURS", "1") or "1"))
    checkout_second_cutoff = now - timedelta(hours=int(os.getenv("RELAY_CHECKOUT_SECOND_FOLLOWUP_HOURS", "24") or "24"))

    messy_due = 0
    messy_second_due = 0
    sample_due = 0
    sample_second_due = 0
    checkout_due = 0
    checkout_second_due = 0

    messy_leads = session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.source.ilike("%messy_notes%"))
        .where(RelayIntentLead.created_at <= messy_cutoff)
        .limit(100)
    ).scalars().all()
    for lead in messy_leads:
        if _is_internal_email(lead.email):
            continue
        if _paid_for_email(session, lead.email):
            continue
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead.id}")
            .where(AcquisitionEvent.event_type == "autopilot_messy_notes_checkout_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is None:
            messy_due += 1

    first_messy_followups = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "autopilot_messy_notes_checkout_followup_sent")
        .where(AcquisitionEvent.created_at <= messy_second_cutoff)
        .limit(100)
    ).scalars().all()
    seen_messy_leads: set[int] = set()
    for followup in first_messy_followups:
        payload = _safe_json(followup.payload_json)
        lead_id = payload.get("relay_lead_id")
        if lead_id is None and followup.prospect_external_id.startswith("relay-lead:"):
            lead_id = followup.prospect_external_id.split(":", 1)[1].strip()
        try:
            lead_id_int = int(lead_id)
        except Exception:
            continue
        if lead_id_int in seen_messy_leads:
            continue
        seen_messy_leads.add(lead_id_int)
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead_id_int}")
            .where(AcquisitionEvent.event_type == "autopilot_messy_notes_second_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is not None:
            continue
        lead = session.execute(
            select(RelayIntentLead).where(RelayIntentLead.id == lead_id_int).limit(1)
        ).scalar_one_or_none()
        if lead is None or not lead.email or _is_internal_email(lead.email) or _paid_for_email(session, lead.email):
            continue
        messy_second_due += 1

    sample_leads = session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.source.ilike("%sample%"))
        .where(RelayIntentLead.created_at <= sample_cutoff)
        .limit(100)
    ).scalars().all()
    for lead in sample_leads:
        if _is_internal_email(lead.email):
            continue
        if _paid_for_email(session, lead.email):
            continue
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead.id}")
            .where(AcquisitionEvent.event_type == "autopilot_sample_notes_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is None:
            sample_due += 1

    first_sample_followups = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "autopilot_sample_notes_followup_sent")
        .where(AcquisitionEvent.created_at <= sample_second_cutoff)
        .limit(100)
    ).scalars().all()
    seen_sample_leads: set[int] = set()
    for followup in first_sample_followups:
        payload = _safe_json(followup.payload_json)
        lead_id = payload.get("relay_lead_id")
        if lead_id is None and followup.prospect_external_id.startswith("relay-lead:"):
            lead_id = followup.prospect_external_id.split(":", 1)[1].strip()
        try:
            lead_id_int = int(lead_id)
        except Exception:
            continue
        if lead_id_int in seen_sample_leads:
            continue
        seen_sample_leads.add(lead_id_int)
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead_id_int}")
            .where(AcquisitionEvent.event_type == "autopilot_sample_second_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is not None:
            continue
        lead = session.execute(
            select(RelayIntentLead).where(RelayIntentLead.id == lead_id_int).limit(1)
        ).scalar_one_or_none()
        if lead is None or not lead.email:
            continue
        email = (lead.email or "").strip().lower()
        if _is_internal_email(email) or _paid_for_email(session, email):
            continue
        messy_notes = session.execute(
            select(RelayIntentLead.id)
            .where(RelayIntentLead.email == email)
            .where(RelayIntentLead.source.ilike("%messy_notes%"))
            .limit(1)
        ).scalar_one_or_none()
        if messy_notes is not None:
            continue
        sample_second_due += 1

    checkout_events = session.execute(
        select(RelayIntentEvent)
        .where(RelayIntentEvent.event_type == "checkout_click")
        .where(RelayIntentEvent.created_at <= checkout_cutoff)
        .limit(100)
    ).scalars().all()
    seen_sessions: set[str] = set()
    for event in checkout_events:
        session_id = (event.session_id or "").strip()
        if not session_id or session_id in seen_sessions:
            continue
        seen_sessions.add(session_id)
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-session:{session_id}")
            .where(AcquisitionEvent.event_type == "autopilot_checkout_intent_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is not None:
            continue
        lead = session.execute(
            select(RelayIntentLead)
            .where(RelayIntentLead.session_id == session_id)
            .order_by(RelayIntentLead.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if lead is None or not lead.email or _is_internal_email(lead.email) or _paid_for_email(session, lead.email):
            continue
        checkout_due += 1

    checkout_leads = session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.source.ilike("%checkout_intent%"))
        .where(RelayIntentLead.created_at <= checkout_cutoff)
        .limit(100)
    ).scalars().all()
    for lead in checkout_leads:
        session_id = (lead.session_id or "").strip()
        if not session_id or session_id in seen_sessions:
            continue
        seen_sessions.add(session_id)
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-session:{session_id}")
            .where(AcquisitionEvent.event_type == "autopilot_checkout_intent_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is not None:
            continue
        if not lead.email or _is_internal_email(lead.email) or _paid_for_email(session, lead.email):
            continue
        checkout_due += 1

    first_checkout_followups = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "autopilot_checkout_intent_followup_sent")
        .where(AcquisitionEvent.created_at <= checkout_second_cutoff)
        .limit(100)
    ).scalars().all()
    seen_second_sessions: set[str] = set()
    for followup in first_checkout_followups:
        payload = _safe_json(followup.payload_json)
        session_id = str(payload.get("session_id") or "").strip()
        if not session_id and followup.prospect_external_id.startswith("relay-session:"):
            session_id = followup.prospect_external_id.split(":", 1)[1].strip()
        if not session_id or session_id in seen_second_sessions:
            continue
        seen_second_sessions.add(session_id)
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-session:{session_id}")
            .where(AcquisitionEvent.event_type == "autopilot_checkout_intent_second_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is not None:
            continue
        lead = session.execute(
            select(RelayIntentLead)
            .where(RelayIntentLead.session_id == session_id)
            .order_by(RelayIntentLead.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if lead is None or not lead.email or _is_internal_email(lead.email) or _paid_for_email(session, lead.email):
            continue
        checkout_second_due += 1

    return {
        "messy_notes_due": messy_due,
        "messy_notes_second_due": messy_second_due,
        "sample_request_due": sample_due,
        "sample_request_second_due": sample_second_due,
        "checkout_intent_due": checkout_due,
        "checkout_intent_second_due": checkout_second_due,
    }


def _env_snapshot() -> dict[str, bool]:
    return {
        "DATABASE_URL": bool(settings.database_url),
        "RESEND_API_KEY": bool(settings.resend_api_key),
        "PACKET_CHECKOUT_URL": bool(settings.packet_checkout_url),
        "CLIENT_INTAKE_DESTINATION": bool(settings.client_intake_destination or os.getenv("CLIENT_INTAKE_URL", "").strip()),
        "FROM_EMAIL_FULFILLMENT": bool(settings.from_email_fulfillment),
        "APOLLO_API_KEY": bool(settings.apollo_api_key),
        "BUYER_MAILBOX_PASSWORD": bool(settings.buyer_acq_mailbox_password),
    }


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


def _experiment_failure_sample() -> int:
    min_sample = _int_env("RELAY_EXPERIMENT_MIN_SAMPLE", 20)
    return max(1, _int_env("RELAY_EXPERIMENT_FAILURE_SAMPLE", min_sample))


def relay_success_snapshot(days: int = 7) -> dict[str, Any]:
    days = max(1, min(int(days), 90))
    now = _now()
    since = now - timedelta(days=days)
    with _session() as session:
        import app.services.custom_outreach as outreach_service

        outreach = outreach_service.outreach_status()
        money = _money_metrics(session, since=since)
        internal_sessions = _internal_session_ids(session, since=since)
        page_views = _intent_count(session, "page_view", since=since, exclude_sessions=internal_sessions)
        checkout_clicks = _intent_count(session, "checkout_click", since=since, exclude_sessions=internal_sessions)
        notes_clicks = _intent_count(session, "note_intake_click", since=since, exclude_sessions=internal_sessions)
        lead_count = _lead_count(session, None, since=since)
        messy_notes = _lead_count(session, "messy_notes", since=since)
        sample_requests = _lead_count(session, "sample", since=since)
        sends = _event_count(session, "custom_outreach_sent_step_%", since=since, like=True)
        replies = (
            _event_count(session, "custom_outreach_reply_seen", since=since)
            + _event_count(session, "smartlead_reply", since=since)
        )
        fulfilled = _event_count(session, "autopilot_paid_relay_notes_fulfilled", since=since)
        onboarding = _event_count(session, "autopilot_paid_onboarding_sent", since=since)
        inbound_followups = (
            _event_count(session, "autopilot_messy_notes_checkout_followup_sent", since=since)
            + _event_count(session, "autopilot_messy_notes_second_followup_sent", since=since)
            + _event_count(session, "autopilot_sample_notes_followup_sent", since=since)
            + _event_count(session, "autopilot_sample_second_followup_sent", since=since)
            + _event_count(session, "autopilot_checkout_intent_followup_sent", since=since)
            + _event_count(session, "autopilot_checkout_intent_second_followup_sent", since=since)
        )
        due_followups = _due_followup_counts(session, now=now)

    env = _env_snapshot()
    critical_missing = [
        name
        for name in ["DATABASE_URL", "RESEND_API_KEY", "PACKET_CHECKOUT_URL", "FROM_EMAIL_FULFILLMENT"]
        if not env.get(name)
    ]

    return {
        "status": "ok",
        "days": days,
        "since": since.isoformat(),
        "env": env,
        "critical_missing": critical_missing,
        "money": money,
        "intent": {
            "page_views": page_views,
            "notes_clicks": notes_clicks,
            "checkout_clicks": checkout_clicks,
            "lead_count": lead_count,
            "messy_notes": messy_notes,
            "sample_requests": sample_requests,
        },
        "outreach": {
            "sends": sends,
            "replies": replies,
            "reply_rate": round(replies / sends, 4) if sends else 0,
            "due_now": int(outreach.get("due_now_count") or outreach.get("queued_count") or 0),
            "sent_today": int(outreach.get("sent_today") or 0),
            "daily_send_cap": int(outreach.get("daily_send_cap") or 0),
            "cap_remaining": int(outreach.get("cap_remaining") or 0),
            "active_experiment_variant": outreach.get("active_experiment_variant", ""),
            "active_experiment_sends": int(outreach.get("active_experiment_sends") or 0),
            "active_experiment_sample_target": int(outreach.get("active_experiment_sample_target") or 0),
            "active_experiment_needs_sample": bool(outreach.get("active_experiment_needs_sample") or False),
            "active_experiment_new_due_count": int(outreach.get("active_experiment_new_due_count") or 0),
            "active_experiment_direct_new_due_count": int(
                outreach.get("active_experiment_direct_new_due_count") or 0
            ),
            "active_experiment_generic_new_due_count": int(
                outreach.get("active_experiment_generic_new_due_count") or 0
            ),
            "active_experiment_allowed_generic_new_due_count": int(
                outreach.get("active_experiment_allowed_generic_new_due_count") or 0
            ),
            "active_experiment_generic_sample_daily_cap": int(
                outreach.get("active_experiment_generic_sample_daily_cap") or 0
            ),
            "send_window_is_open": bool(outreach.get("send_window_is_open") or False),
            "send_window_reason": outreach.get("send_window_reason", ""),
            "send_window_next_open_local": outreach.get("send_window_next_open_local", ""),
            "next_money_move": outreach.get("next_money_move", ""),
        },
        "conversion": {
            "inbound_followups_sent": inbound_followups,
            "messy_notes_followups_due": due_followups["messy_notes_due"],
            "messy_notes_second_followups_due": due_followups["messy_notes_second_due"],
            "sample_followups_due": due_followups["sample_request_due"],
            "sample_second_followups_due": due_followups["sample_request_second_due"],
            "checkout_followups_due": due_followups["checkout_intent_due"],
            "checkout_second_followups_due": due_followups["checkout_intent_second_due"],
            "paid_onboarding_sent": onboarding,
            "paid_notes_fulfilled": fulfilled,
        },
    }


def _bottleneck(snapshot: dict[str, Any]) -> str:
    if snapshot.get("critical_missing"):
        return "infrastructure_blocked"

    money = snapshot["money"]
    intent = snapshot["intent"]
    outreach = snapshot["outreach"]
    conversion = snapshot["conversion"]

    if int(money.get("payments") or 0) > 0 and int(conversion.get("paid_notes_fulfilled") or 0) < int(money.get("payments") or 0):
        return "paid_fulfillment"
    if int(conversion.get("messy_notes_followups_due") or 0) > 0:
        return "messy_notes_to_payment"
    if int(conversion.get("messy_notes_second_followups_due") or 0) > 0:
        return "messy_notes_to_payment"
    if int(conversion.get("sample_followups_due") or 0) > 0:
        return "sample_to_notes"
    if int(conversion.get("sample_second_followups_due") or 0) > 0:
        return "sample_to_notes"
    if int(conversion.get("checkout_followups_due") or 0) > 0:
        return "checkout_to_payment"
    if int(conversion.get("checkout_second_followups_due") or 0) > 0:
        return "checkout_to_payment"
    if int(intent.get("checkout_clicks") or 0) > int(money.get("payments") or 0):
        return "checkout_to_payment"
    if outreach.get("active_experiment_needs_sample"):
        if int(outreach.get("active_experiment_new_due_count") or 0) > 0:
            return "active_experiment_sample"
        return "active_experiment_refill"
    if int(intent.get("lead_count") or 0) == 0 and int(intent.get("page_views") or 0) >= 20:
        return "page_to_lead"
    if int(intent.get("page_views") or 0) < 20 and int(outreach.get("sends") or 0) < 20:
        return "traffic"
    if int(outreach.get("sends") or 0) >= _experiment_failure_sample() and int(outreach.get("replies") or 0) == 0:
        return "outbound_targeting_or_copy"
    if int(outreach.get("due_now") or 0) == 0 and int(outreach.get("cap_remaining") or 0) > 0:
        return "lead_refill"
    return "running"


def _next_action(bottleneck: str) -> str:
    actions = {
        "infrastructure_blocked": "Fix missing production credentials before trying to scale.",
        "paid_fulfillment": "Fulfill paid buyers and keep reminders active until delivery is complete.",
        "messy_notes_to_payment": "Send the notes-to-checkout follow-up.",
        "sample_to_notes": "Send the sample-to-notes follow-up.",
        "checkout_to_payment": "Keep notes-first friction low and make the paid test obvious after interest.",
        "active_experiment_sample": "Collect the active outbound experiment sample before judging the offer.",
        "active_experiment_refill": "Refill fresh first-touch leads for the active outbound experiment.",
        "page_to_lead": "Improve the first-screen ask before changing the backend.",
        "traffic": "Let direct-buyer outbound refill and send; the system needs more qualified traffic.",
        "outbound_targeting_or_copy": "Do not scale volume; rotate one controlled experiment and target direct buyers only.",
        "lead_refill": "Refill direct decision-maker leads.",
        "running": "Keep the loop steady and avoid random changes.",
    }
    return actions.get(bottleneck, actions["running"])


def _run_outbound_experiment_review_if_needed(bottleneck: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    if bottleneck != "outbound_targeting_or_copy":
        return {"status": "skipped", "summary": "outbound experiment review not needed for this bottleneck"}

    failure_sample = _experiment_failure_sample()
    sends = int(snapshot.get("outreach", {}).get("sends") or 0)
    replies = int(snapshot.get("outreach", {}).get("replies") or 0)
    payments = int(snapshot.get("money", {}).get("payments") or 0)
    if sends < failure_sample:
        return {"status": "skipped", "summary": "outbound sample is not large enough to rotate yet"}
    if replies > 0 or payments > 0:
        return {"status": "skipped", "summary": "signal exists; keep the current outbound lane stable"}

    performance = relay_performance_status()
    active_plan = performance.get("active_experiment") or {}
    active_signal = performance.get("active_experiment_signal") or {}
    active_variant = str(active_plan.get("experiment_variant") or active_signal.get("variant") or "")
    if active_plan.get("source") == "env":
        return {
            "status": "skipped",
            "summary": "outbound variant is pinned by environment",
            "active_variant": active_variant,
        }

    active_sends = int(active_signal.get("sends") or 0)
    active_replies = int(active_signal.get("replies") or 0)
    active_payments = int(active_signal.get("payments") or 0)
    if active_sends < failure_sample:
        return {
            "status": "skipped",
            "summary": "active experiment still needs its own measurable send sample",
            "active_variant": active_variant,
            "active_experiment_sends": active_sends,
            "active_experiment_replies": active_replies,
            "aggregate_sends": sends,
            "failure_sample": failure_sample,
        }
    if active_replies > 0 or active_payments > 0:
        return {
            "status": "skipped",
            "summary": "active experiment has signal; keep collecting evidence",
            "active_variant": active_variant,
            "active_experiment_sends": active_sends,
            "active_experiment_replies": active_replies,
            "active_experiment_payments": active_payments,
        }

    review = run_weekly_performance_review(force=True, fetch_research=False)
    plan = review.get("plan") or {}
    return {
        "status": review.get("status", "ok"),
        "summary": "created active-experiment failure review",
        "created": bool(review.get("created")),
        "previous_experiment_variant": active_variant,
        "previous_experiment_sends": active_sends,
        "previous_experiment_replies": active_replies,
        "failure_sample": failure_sample,
        "experiment_variant": plan.get("experiment_variant"),
        "experiment_label": plan.get("experiment_label"),
        "decision_reasons": plan.get("decision_reasons", []),
        "daily_cap_recommendation": plan.get("daily_cap_recommendation"),
    }


def run_relay_success_control_tick() -> dict[str, Any]:
    before = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(before)

    actions: dict[str, Any] = {}
    actions["inbound_conversion"] = run_inbound_conversion_sweep()
    actions["paid_intake_reminders"] = run_paid_intake_reminder_sweep(
        hours=int(os.getenv("OPS_INTAKE_REMINDER_HOURS", "12") or "12")
    )
    actions["post_delivery_upsell"] = run_post_delivery_upsell_sweep(
        hours=int(os.getenv("OPS_UPSELL_DELAY_HOURS", "24") or "24")
    )
    actions["outbound_experiment_review"] = _run_outbound_experiment_review_if_needed(bottleneck, before)

    after = relay_success_snapshot(days=7)
    result = {
        "status": "ok",
        "bottleneck": bottleneck,
        "next_action": _next_action(bottleneck),
        "before": before,
        "actions": actions,
        "after": after,
        "created_at": _now().isoformat(),
    }

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type=SUCCESS_TICK_EVENT,
                prospect_external_id="relay-success",
                summary=f"{bottleneck}: {_next_action(bottleneck)}",
                payload_json=json.dumps(result, ensure_ascii=False),
            )
        )
        session.commit()

    return result


def relay_success_status() -> dict[str, Any]:
    snapshot = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(snapshot)
    with _session() as session:
        latest = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == SUCCESS_TICK_EVENT)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    return {
        "status": "ok",
        "bottleneck": bottleneck,
        "next_action": _next_action(bottleneck),
        "snapshot": snapshot,
        "latest_tick": {
            "created_at": latest.created_at.isoformat(),
            "summary": latest.summary,
            "payload": _safe_json(latest.payload_json),
        }
        if latest
        else None,
    }
