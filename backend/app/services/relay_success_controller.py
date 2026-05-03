from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
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
INTAKE_SMOKE_EVENT = "relay_intake_smoke_test"
DELIVERY_SMOKE_EVENT = "relay_delivery_smoke_test"


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


def _latest_event_payload(session: Session, event_type: str, *, since: datetime) -> dict[str, Any]:
    event = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == event_type)
        .where(AcquisitionEvent.created_at >= since)
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if event is None:
        return {}
    payload = _safe_json(event.payload_json)
    return {
        "created_at": event.created_at.isoformat() if event.created_at else "",
        "summary": event.summary,
        "prospect_external_id": event.prospect_external_id,
        "payload": payload,
        "error": str(payload.get("error") or "")[:500],
        "error_type": str(payload.get("error_type") or ""),
        "experiment_variant": str(payload.get("experiment_variant") or payload.get("active_experiment_variant") or ""),
    }


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
        "OPENAI_API_KEY": bool(settings.openai_api_key or os.getenv("OPENAI_API_KEY", "").strip()),
        "RESEND_API_KEY": bool(settings.resend_api_key),
        "STRIPE_WEBHOOK_SECRET": bool(settings.stripe_webhook_secret),
        "TALLY_WEBHOOK_SECRET": bool(settings.tally_webhook_secret),
        "PACKET_CHECKOUT_URL": bool(settings.packet_checkout_url),
        "PACKET_5_PACK_URL": bool(
            os.getenv("PACKET_5_PACK_URL", "").strip() or getattr(settings, "packet_5_pack_url", "")
        ),
        "WEEKLY_SPRINT_URL": bool(
            os.getenv("WEEKLY_SPRINT_URL", "").strip() or getattr(settings, "weekly_sprint_url", "")
        ),
        "MONTHLY_AUTOPILOT_URL": bool(
            os.getenv("MONTHLY_AUTOPILOT_URL", "").strip() or getattr(settings, "monthly_autopilot_url", "")
        ),
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


def _intake_smoke_interval_hours() -> int:
    return max(_int_env("RELAY_INTAKE_SMOKE_INTERVAL_HOURS", 24), 1)


def _delivery_smoke_interval_hours() -> int:
    return max(_int_env("RELAY_DELIVERY_SMOKE_INTERVAL_HOURS", 24), 1)


def _run_intake_smoke_check_if_needed() -> dict[str, Any]:
    now = _now()
    interval_hours = _intake_smoke_interval_hours()
    with _session() as session:
        latest = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == INTAKE_SMOKE_EVENT)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest is not None and latest.created_at is not None:
            age_seconds = max(int((now - latest.created_at.replace(tzinfo=None)).total_seconds()), 0)
            latest_payload = _safe_json(latest.payload_json)
            if latest_payload.get("status") != "error" and age_seconds < interval_hours * 3600:
                return {
                    "status": "skipped",
                    "summary": "recent_intake_smoke_check_exists",
                    "latest_at": latest.created_at.isoformat(),
                    "age_seconds": age_seconds,
                    "interval_hours": interval_hours,
                }

        env = _env_snapshot()
        missing = [
            name
            for name in [
                "TALLY_WEBHOOK_SECRET",
                "CLIENT_INTAKE_DESTINATION",
                "FROM_EMAIL_FULFILLMENT",
            ]
            if not env.get(name)
        ]
        status = "ok" if not missing else "error"
        summary = "intake_smoke_config_ok" if not missing else "intake_smoke_missing_config"
        payload = {
            "status": status,
            "missing": missing,
            "route": "/webhooks/tally",
            "client_intake_destination_configured": env.get("CLIENT_INTAKE_DESTINATION"),
            "did_not_create_payment": True,
            "did_not_send_customer_email": True,
            "checked_at": now.isoformat(),
        }
        session.add(
            AcquisitionEvent(
                event_type=INTAKE_SMOKE_EVENT,
                prospect_external_id="relay-intake-smoke",
                summary=summary,
                payload_json=json.dumps(payload, ensure_ascii=False),
            )
        )
        session.commit()
        return {
            "status": status,
            "summary": summary,
            "missing": missing,
            "interval_hours": interval_hours,
        }


def _run_delivery_smoke_check_if_needed() -> dict[str, Any]:
    now = _now()
    interval_hours = _delivery_smoke_interval_hours()
    with _session() as session:
        latest = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == DELIVERY_SMOKE_EVENT)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest is not None and latest.created_at is not None:
            age_seconds = max(int((now - latest.created_at.replace(tzinfo=None)).total_seconds()), 0)
            latest_payload = _safe_json(latest.payload_json)
            if latest_payload.get("status") != "error" and age_seconds < interval_hours * 3600:
                return {
                    "status": "skipped",
                    "summary": "recent_delivery_smoke_check_exists",
                    "latest_at": latest.created_at.isoformat(),
                    "age_seconds": age_seconds,
                    "interval_hours": interval_hours,
                }

        env = _env_snapshot()
        missing = [
            name
            for name in ["RESEND_API_KEY", "FROM_EMAIL_FULFILLMENT"]
            if not env.get(name)
        ]
        detail: dict[str, Any] = {
            "did_not_create_payment": True,
            "did_not_generate_packet": True,
            "did_not_send_customer_email": True,
            "checked_at": now.isoformat(),
        }
        try:
            from app.workers import fulfillment

            master_status = fulfillment.ensure_master_workbook()
            master_path = fulfillment.MASTER_PATH
            generate_script = fulfillment.GENERATE_SCRIPT
            detail["master_path"] = str(master_path)
            detail["generate_script"] = str(generate_script)
            detail["master_workbook_status"] = master_status
            detail["master_path_exists"] = master_path.exists()
            detail["generate_script_exists"] = generate_script.exists()
            detail["builtin_generator_available"] = hasattr(fulfillment, "_run_builtin_generator")
            if not master_path.exists():
                missing.append("FULFILLMENT_MASTER_WORKBOOK")
            if not generate_script.exists() and not hasattr(fulfillment, "_run_builtin_generator"):
                missing.append("FULFILLMENT_GENERATE_SCRIPT")
            if master_path.exists():
                wb = fulfillment.load_workbook(master_path, read_only=True)
                try:
                    if "Master Log" not in wb.sheetnames:
                        missing.append("FULFILLMENT_MASTER_LOG_SHEET")
                        detail["master_sheet_exists"] = False
                    else:
                        ws = wb["Master Log"]
                        headers = {
                            str(ws.cell(1, col).value)
                            for col in range(1, ws.max_column + 1)
                            if ws.cell(1, col).value
                        }
                        missing_headers = [h for h in fulfillment.MASTER_HEADERS if h not in headers]
                        detail["master_sheet_exists"] = True
                        detail["missing_master_headers"] = missing_headers
                        if missing_headers:
                            missing.append("FULFILLMENT_MASTER_HEADERS")
                finally:
                    wb.close()
        except Exception as exc:
            missing.append("FULFILLMENT_SMOKE_EXCEPTION")
            detail["error_type"] = type(exc).__name__
            detail["error"] = str(exc)[:500]

        missing = sorted(set(missing))
        status = "ok" if not missing else "error"
        summary = "delivery_smoke_config_ok" if not missing else "delivery_smoke_missing_config"
        payload = {
            "status": status,
            "missing": missing,
            **detail,
        }
        session.add(
            AcquisitionEvent(
                event_type=DELIVERY_SMOKE_EVENT,
                prospect_external_id="relay-delivery-smoke",
                summary=summary,
                payload_json=json.dumps(payload, ensure_ascii=False),
            )
        )
        session.commit()
        return {
            "status": status,
            "summary": summary,
            "missing": missing,
            "interval_hours": interval_hours,
        }


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _send_window_stall_grace_seconds() -> int:
    return max(_int_env("RELAY_SEND_WINDOW_STALL_GRACE_MINUTES", 10), 1) * 60


def _send_window_seconds_open(outreach: dict[str, Any]) -> int:
    if not bool(outreach.get("send_window_is_open")):
        return 0
    raw_seconds_open = outreach.get("send_window_seconds_open")
    if raw_seconds_open is not None:
        try:
            return max(int(raw_seconds_open), 0)
        except Exception:
            pass
    now_local = _parse_iso_datetime(outreach.get("send_window_now_local"))
    start_local = _parse_iso_datetime(outreach.get("send_window_start_local"))
    if now_local is None or start_local is None:
        return 0
    try:
        return max(int((now_local - start_local).total_seconds()), 0)
    except Exception:
        return 0


def _outbound_send_stalled(outreach: dict[str, Any]) -> bool:
    if not bool(outreach.get("send_window_is_open")):
        return False
    if int(outreach.get("sent_today") or 0) > 0:
        return False
    if int(outreach.get("cap_remaining") or 0) <= 0:
        return False
    if int(outreach.get("due_now") or 0) <= 0:
        return False
    return int(outreach.get("send_window_seconds_open") or 0) >= _send_window_stall_grace_seconds()


def _outbound_send_window_missed(outreach: dict[str, Any]) -> bool:
    if str(outreach.get("send_window_reason") or "") != "after_window":
        return False
    if int(outreach.get("sent_today") or 0) > 0:
        return False
    if int(outreach.get("cap_remaining") or 0) <= 0:
        return False
    due = int(outreach.get("active_experiment_new_due_count") or outreach.get("due_now") or 0)
    return due > 0


def _outbound_send_window_underfilled(outreach: dict[str, Any]) -> bool:
    if str(outreach.get("send_window_reason") or "") != "after_window":
        return False
    if int(outreach.get("sent_today") or 0) <= 0:
        return False
    if int(outreach.get("cap_remaining") or 0) <= 0:
        return False
    due = int(outreach.get("active_experiment_new_due_count") or outreach.get("due_now") or 0)
    return due > 0


def _outbound_window_audit_at(outreach: dict[str, Any]) -> str:
    reason = str(outreach.get("send_window_reason") or "").strip()
    end_local = str(outreach.get("send_window_end_local") or "").strip()
    if reason == "open" and end_local:
        return end_local

    next_dt = _parse_iso_datetime(outreach.get("send_window_next_open_local"))
    start_dt = _parse_iso_datetime(outreach.get("send_window_start_local"))
    end_dt = _parse_iso_datetime(outreach.get("send_window_end_local"))
    if next_dt is not None and start_dt is not None and end_dt is not None:
        duration = end_dt - start_dt
        if duration.total_seconds() > 0:
            return (next_dt + duration).isoformat()
    return str(outreach.get("send_window_end_local") or outreach.get("send_window_next_open_local") or "").strip()


def _outbound_window_execution_contract(outreach: dict[str, Any]) -> dict[str, Any]:
    active_sends = int(outreach.get("active_experiment_sends") or 0)
    active_target = int(outreach.get("active_experiment_sample_target") or 0)
    active_due = int(outreach.get("active_experiment_new_due_count") or outreach.get("due_now_count") or 0)
    active_remaining = max(active_target - active_sends, 0) if active_target else 0
    cap_remaining = int(outreach.get("cap_remaining") or 0)
    daily_cap = int(outreach.get("daily_send_cap") or 0)
    capacity = max(min(cap_remaining or daily_cap, daily_cap or cap_remaining or 1), 1)
    expected_sends = (
        min(active_remaining, active_due, capacity)
        if active_remaining > 0 and active_due > 0 and capacity > 0
        else 0
    )
    expected_after = min(active_sends + expected_sends, active_target) if active_target else active_sends
    expected_progress = f"{expected_after}/{active_target}" if active_target else ""
    reason = str(outreach.get("send_window_reason") or "").strip()
    sent_today = int(outreach.get("sent_today") or 0)

    if active_remaining <= 0:
        state = "sample_complete"
    elif expected_sends <= 0:
        state = "blocked"
    elif reason == "open":
        state = "window_open"
    elif reason == "after_window" and active_due > 0 and cap_remaining > 0 and sent_today <= 0:
        state = "window_missed"
    elif reason == "after_window" and active_due > 0 and cap_remaining > 0:
        state = "window_underfilled"
    elif reason == "after_window":
        state = "window_passed"
    else:
        state = "waiting_for_window"

    if expected_sends > 0:
        success_criterion = (
            f"send {expected_sends} active leads and move progress "
            f"from {active_sends}/{active_target} to {expected_progress}"
        )
        failure_condition = (
            f"after audit time, interrupt if fewer than {expected_sends} active sends completed "
            "or the window closes with queued active leads and unused capacity"
        )
    else:
        success_criterion = "make queued active leads and send capacity available"
        failure_condition = "interrupt if the loop cannot create queued active leads or send capacity"

    return {
        "state": state,
        "expected_sends": expected_sends,
        "expected_progress": expected_progress,
        "audit_at": _outbound_window_audit_at(outreach),
        "success_criterion": success_criterion,
        "failure_condition": failure_condition,
    }


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
        send_failures = _event_count(session, "custom_outreach_send_failed", since=since)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        send_failures_today = _event_count(session, "custom_outreach_send_failed", since=today_start)
        latest_send_failure = _latest_event_payload(session, "custom_outreach_send_failed", since=since)
        replies = (
            _event_count(session, "custom_outreach_reply_seen", since=since)
            + _event_count(session, "smartlead_reply", since=since)
        )
        auto_replies = _event_count(session, "custom_outreach_auto_reply_sent", since=since)
        unhandled_replies = max(replies - auto_replies - int(money.get("payments") or 0), 0)
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

    try:
        performance = relay_performance_status()
    except Exception as error:
        performance = {"status": "error", "error": str(error)}

    send_window_seconds_open = _send_window_seconds_open(outreach)
    due_now = int(outreach.get("due_now_count") or outreach.get("queued_count") or 0)
    sent_today = int(outreach.get("sent_today") or 0)
    cap_remaining = int(outreach.get("cap_remaining") or 0)
    send_window_stall_grace_seconds = _send_window_stall_grace_seconds()
    outbound_send_stalled = (
        bool(outreach.get("send_window_is_open") or False)
        and sent_today <= 0
        and cap_remaining > 0
        and due_now > 0
        and send_window_seconds_open >= send_window_stall_grace_seconds
    )
    window_execution_contract = _outbound_window_execution_contract(outreach)
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
            "send_failures": send_failures,
            "send_failures_today": send_failures_today,
            "latest_send_failure": latest_send_failure,
            "replies": replies,
            "auto_replies": auto_replies,
            "unhandled_replies": unhandled_replies,
            "reply_to_payment_gap": unhandled_replies,
            "auto_closed_replies": min(auto_replies, replies),
            "reply_rate": round(replies / sends, 4) if sends else 0,
            "due_now": due_now,
            "sent_today": sent_today,
            "daily_send_cap": int(outreach.get("daily_send_cap") or 0),
            "cap_remaining": cap_remaining,
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
            "send_window_now_local": outreach.get("send_window_now_local", ""),
            "send_window_start_local": outreach.get("send_window_start_local", ""),
            "send_window_end_local": outreach.get("send_window_end_local", ""),
            "send_window_next_open_local": outreach.get("send_window_next_open_local", ""),
            "send_window_seconds_until_open": int(outreach.get("send_window_seconds_until_open") or 0),
            "send_window_seconds_open": send_window_seconds_open,
            "send_window_stall_grace_seconds": send_window_stall_grace_seconds,
            "outbound_send_stalled": outbound_send_stalled,
            "window_execution_contract": window_execution_contract,
            "window_execution_state": window_execution_contract.get("state"),
            "next_window_audit_at": window_execution_contract.get("audit_at"),
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
        "performance": performance,
    }


def _progress_current(value: Any) -> int:
    text = str(value or "").strip()
    if "/" not in text:
        return 0
    return _safe_int(text.split("/", 1)[0])


def _expected_active_sends_for_proof(window_contract: dict[str, Any]) -> int:
    expected_active_sends = _progress_current(window_contract.get("expected_progress"))
    if expected_active_sends > 0:
        return expected_active_sends
    success_criterion = str(window_contract.get("success_criterion") or "").strip()
    if " to " in success_criterion:
        expected_active_sends = _progress_current(success_criterion.rsplit(" to ", 1)[-1])
        if expected_active_sends > 0:
            return expected_active_sends
    return _safe_int(window_contract.get("expected_sends"))


def _active_sample_execution_proof_missed(outreach: dict[str, Any], active_sends: int) -> bool:
    window_contract = (
        outreach.get("window_execution_contract")
        if isinstance(outreach.get("window_execution_contract"), dict)
        else {}
    )
    deadline = window_contract.get("audit_at") or outreach.get("next_window_audit_at") or ""
    deadline_at = _parse_proof_datetime(deadline)
    if deadline_at is None or datetime.now(timezone.utc) <= deadline_at:
        return False
    expected_active_sends = _expected_active_sends_for_proof(window_contract)
    return expected_active_sends > 0 and active_sends < expected_active_sends


def _bottleneck(snapshot: dict[str, Any]) -> str:
    if snapshot.get("critical_missing"):
        return "infrastructure_blocked"

    money = snapshot["money"]
    intent = snapshot["intent"]
    outreach = snapshot["outreach"]
    conversion = snapshot["conversion"]
    performance = snapshot.get("performance") if isinstance(snapshot.get("performance"), dict) else {}
    performance_ok = performance.get("status") == "ok"
    active_signal = performance.get("active_experiment_signal") if isinstance(performance.get("active_experiment_signal"), dict) else {}
    active_sends = int(active_signal.get("sends") or outreach.get("active_experiment_sends") or 0)
    active_target = int(outreach.get("active_experiment_sample_target") or _experiment_failure_sample())
    active_replies = int(active_signal.get("replies") or 0)
    active_payments = int(active_signal.get("payments") or 0)
    active_sample_complete_without_signal = (
        performance_ok
        and active_target > 0
        and active_sends >= active_target
        and active_replies <= 0
        and active_payments <= 0
    )
    unhandled_replies = int(
        outreach.get("unhandled_replies")
        if outreach.get("unhandled_replies") is not None
        else max(
            int(outreach.get("replies") or 0)
            - int(outreach.get("auto_replies") or 0)
            - int(money.get("payments") or 0),
            0,
        )
    )

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
    if (
        unhandled_replies > 0
        and not active_sample_complete_without_signal
    ):
        return "reply_to_payment"
    if int(money.get("payments") or 0) > 0:
        return "paid_signal_keep_stable"
    if active_replies > active_payments:
        return "active_signal_to_payment"
    if _active_sample_execution_proof_missed(outreach, active_sends):
        return "active_sample_execution_missed"
    if (
        int(outreach.get("send_failures_today") or 0) > 0
        and int(outreach.get("sent_today") or 0) == 0
        and int(outreach.get("due_now") or 0) > 0
        and int(outreach.get("cap_remaining") or 0) > 0
        and str(outreach.get("send_window_reason") or "") not in {"weekend", "before_window"}
    ):
        return "outbound_send_failed"
    if _outbound_send_stalled(outreach):
        return "outbound_send_stalled"
    if _outbound_send_window_missed(outreach):
        return "outbound_window_missed"
    if _outbound_send_window_underfilled(outreach):
        return "outbound_window_underfilled"
    if outreach.get("active_experiment_needs_sample"):
        if int(outreach.get("active_experiment_new_due_count") or 0) > 0:
            return "active_experiment_sample"
        return "active_experiment_refill"
    if int(intent.get("lead_count") or 0) == 0 and int(intent.get("page_views") or 0) >= 20:
        return "page_to_lead"
    if int(intent.get("page_views") or 0) < 20 and int(outreach.get("sends") or 0) < 20:
        return "traffic"
    if active_sample_complete_without_signal:
        return "outbound_targeting_or_copy"
    if int(outreach.get("sends") or 0) >= _experiment_failure_sample() and int(outreach.get("replies") or 0) == 0:
        return "outbound_targeting_or_copy"
    if int(outreach.get("due_now") or 0) == 0 and int(outreach.get("cap_remaining") or 0) > 0:
        return "lead_refill"
    return "running"


def _next_action(bottleneck: str) -> str:
    actions = {
        "infrastructure_blocked": "Fix missing production credentials before trying to scale.",
        "paid_fulfillment": "Fulfill paid buyers and keep reminders active until delivery is complete.",
        "paid_signal_keep_stable": "Keep the paid lane stable; continue only controlled tests that do not disturb fulfillment.",
        "messy_notes_to_payment": "Send the notes-to-checkout follow-up.",
        "sample_to_notes": "Send the sample-to-notes follow-up.",
        "checkout_to_payment": "Keep notes-first friction low and make the paid test obvious after interest.",
        "reply_to_payment": "Close real replies through the paid next step before changing traffic or copy.",
        "active_signal_to_payment": "Keep the active lane stable and convert active replies to checkout or payment.",
        "outbound_send_failed": (
            "Sender failed today before any email was sent; use SMTP failover details "
            "and fix the sending lane before judging demand."
        ),
        "outbound_send_stalled": (
            "Send window is open with queued leads and capacity, but zero sends; "
            "retry sender and inspect the latest outreach failure."
        ),
        "outbound_window_missed": (
            "Send window closed with queued leads and capacity but zero sends; "
            "treat it as an execution miss and inspect the money loop before waiting another day."
        ),
        "outbound_window_underfilled": (
            "Send window closed with queued leads and unused capacity; "
            "treat the partial send as an execution miss before judging demand."
        ),
        "active_sample_execution_missed": (
            "Active sample proof missed its audit deadline; recover execution before judging demand."
        ),
        "active_experiment_sample": "Collect the active outbound experiment sample before judging the offer.",
        "active_experiment_refill": "Refill fresh first-touch leads for the active outbound experiment.",
        "page_to_lead": "Improve the first-screen ask before changing the backend.",
        "traffic": "Let direct-buyer outbound refill and send; the system needs more qualified traffic.",
        "outbound_targeting_or_copy": "Do not scale volume; rotate one controlled experiment and target direct buyers only.",
        "lead_refill": "Refill direct decision-maker leads.",
        "running": "Keep the loop steady and avoid random changes.",
    }
    return actions.get(bottleneck, actions["running"])


def _money_proof_mandate(snapshot: dict[str, Any], bottleneck: str) -> dict[str, Any]:
    money = snapshot.get("money") if isinstance(snapshot.get("money"), dict) else {}
    outreach = snapshot.get("outreach") if isinstance(snapshot.get("outreach"), dict) else {}
    intent = snapshot.get("intent") if isinstance(snapshot.get("intent"), dict) else {}
    conversion = snapshot.get("conversion") if isinstance(snapshot.get("conversion"), dict) else {}
    window_contract = (
        outreach.get("window_execution_contract")
        if isinstance(outreach.get("window_execution_contract"), dict)
        else {}
    )
    gross_usd = round(_safe_float(money.get("gross_usd")), 2)
    payments = int(money.get("payments") or 0)
    weekly_target_usd = _safe_float(os.getenv("RELAY_WEEKLY_TARGET_USD", "100"), 100.0)
    active_sends = int(outreach.get("active_experiment_sends") or 0)
    active_target = int(outreach.get("active_experiment_sample_target") or _experiment_failure_sample())
    active_remaining = max(active_target - active_sends, 0) if active_target else 0
    expected_sends = int(window_contract.get("expected_sends") or 0)
    expected_active_sends = active_sends + expected_sends if expected_sends > 0 else active_sends
    checkout_gap = max(int(intent.get("checkout_clicks") or 0) - payments, 0)
    unhandled_replies = int(outreach.get("unhandled_replies") or 0)
    fulfilled = int(conversion.get("paid_notes_fulfilled") or 0)

    if snapshot.get("critical_missing"):
        state = "restore_revenue_loop"
        primary_action = "fix missing production credentials before trying to scale"
        owner_policy = "manual_input_required"
    elif payments > fulfilled:
        state = "fulfill_paid_buyer"
        primary_action = "fulfill paid buyers and keep the current lane stable"
        owner_policy = "manual_input_required"
    elif checkout_gap > 0 or bottleneck == "checkout_to_payment":
        state = "close_checkout_intent"
        primary_action = "follow up checkout intent until payment or no-signal timeout"
        owner_policy = "owner_out_of_loop"
    elif unhandled_replies > 0 or bottleneck == "reply_to_payment":
        state = "close_buyer_reply"
        primary_action = "close real replies through the paid next step before changing traffic or copy"
        owner_policy = "manual_input_required"
    elif bottleneck == "active_signal_to_payment":
        state = "convert_active_signal"
        primary_action = "keep the active lane stable and convert active replies to checkout or payment before changing copy or target"
        owner_policy = "owner_out_of_loop"
    elif payments > 0 or bottleneck == "paid_signal_keep_stable":
        state = "protect_winning_lane"
        primary_action = "keep the paid lane stable and continue only controlled tests that do not disturb fulfillment"
        owner_policy = "owner_out_of_loop"
    elif bottleneck in {
        "active_sample_execution_missed",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
        "outbound_window_underfilled",
    }:
        state = "restore_send_execution"
        primary_action = _next_action(bottleneck)
        owner_policy = "manual_input_required"
    elif active_remaining > 0 and expected_sends > 0:
        state = "prove_active_sample"
        expected_progress = window_contract.get("expected_progress") or f"{min(active_sends + expected_sends, active_target)}/{active_target}"
        primary_action = f"send {expected_sends} active leads and move progress from {active_sends}/{active_target} to {expected_progress}"
        owner_policy = "owner_out_of_loop"
    elif bottleneck in {"active_experiment_refill", "lead_refill", "traffic"}:
        state = "refill_direct_buyer_leads"
        primary_action = _next_action(bottleneck)
        owner_policy = "owner_out_of_loop"
    elif bottleneck == "outbound_targeting_or_copy":
        state = "rotate_one_variable"
        primary_action = _next_action(bottleneck)
        owner_policy = "owner_out_of_loop"
    else:
        state = "monitor_money_loop"
        primary_action = _next_action(bottleneck)
        owner_policy = "owner_out_of_loop"

    return {
        "state": state,
        "money_truth": "monetized" if payments > 0 or gross_usd > 0 else "not_monetized_yet",
        "bottleneck": bottleneck,
        "primary_action": primary_action,
        "owner_policy": owner_policy,
        "score": {
            "gross_usd": gross_usd,
            "payments": payments,
            "weekly_target_usd": weekly_target_usd,
            "revenue_gap_usd": round(max(weekly_target_usd - gross_usd, 0), 2),
            "active_experiment_progress": f"{active_sends}/{active_target}" if active_target else "",
            "actual_active_sends": active_sends,
            "expected_next_sends": expected_sends,
            "expected_active_sends": expected_active_sends,
            "active_experiment_remaining": active_remaining,
            "unhandled_replies": unhandled_replies,
            "checkout_to_payment_gap": checkout_gap,
        },
        "proof_deadline": window_contract.get("audit_at") or outreach.get("next_window_audit_at") or "",
        "success_condition": (
            "weekly revenue target met"
            if gross_usd >= weekly_target_usd
            else f"collect paid tests until ${weekly_target_usd:.2f}/week is reached"
        ),
        "allowed_autonomous_action": primary_action,
        "forbidden_until_proof": [
            "do not ask Alan to choose the next move while owner_policy is owner_out_of_loop",
            "do not increase volume before buyer signal or completed sample review",
            "do not change more than one targeting, copy, price, or volume variable at a time",
            "do not declare failure from an execution miss",
        ],
    }


def _parse_proof_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _money_proof_health(mandate: dict[str, Any]) -> dict[str, Any]:
    score = mandate.get("score") if isinstance(mandate.get("score"), dict) else {}
    state = str(mandate.get("state") or "").strip()
    payments = _safe_int(score.get("payments"))
    checkout_gap = _safe_int(score.get("checkout_to_payment_gap"))
    unhandled_replies = _safe_int(score.get("unhandled_replies"))
    actual_active_sends = _safe_int(score.get("actual_active_sends"))
    expected_active_sends = _safe_int(score.get("expected_active_sends"))
    deadline = str(mandate.get("proof_deadline") or "").strip()
    deadline_at = _parse_proof_datetime(deadline)
    now = datetime.now(timezone.utc)
    overdue = deadline_at is not None and now > deadline_at
    seconds_until_deadline = int((deadline_at - now).total_seconds()) if deadline_at is not None else None

    if state == "fulfill_paid_buyer":
        health = "paid_fulfillment_open"
        reason = "payment exists and fulfillment is not complete"
        recovery = "fulfill the paid buyer and keep the current lane stable"
    elif state == "protect_winning_lane":
        health = "winning_lane_active"
        reason = "payment exists; protect the lane that produced money"
        recovery = "keep the paid lane stable and continue only controlled tests"
    elif payments > 0:
        health = "money_proof_satisfied"
        reason = "payment exists"
        recovery = "keep the paid lane stable"
    elif state == "convert_active_signal":
        health = "active_signal_open"
        reason = "active sample has buyer signal ahead of payment"
        recovery = "keep the active lane stable and convert replies to checkout or payment"
    elif state == "rotate_one_variable":
        health = "rotation_required"
        reason = "completed sample has no buyer signal or payment"
        recovery = "rotate exactly one controlled copy or targeting variable before the next sample"
    elif checkout_gap > 0 or unhandled_replies > 0:
        health = "buyer_signal_open"
        reason = "buyer signal is ahead of payment"
        recovery = "close buyer signal through the paid test before changing the experiment"
    elif state == "prove_active_sample" and expected_active_sends > 0 and actual_active_sends >= expected_active_sends:
        health = "execution_proof_satisfied"
        reason = f"active sends reached {actual_active_sends}, meeting the expected proof of {expected_active_sends}"
        recovery = "continue the active sample without changing variables"
    elif state == "prove_active_sample" and overdue:
        health = "execution_proof_missed"
        reason = f"proof deadline passed before active sends reached {expected_active_sends}"
        recovery = "treat this as an execution miss, run recovery, and do not judge demand"
    elif state in {"restore_revenue_loop", "restore_send_execution"}:
        health = "recovery_required"
        reason = str(mandate.get("primary_action") or "revenue loop needs recovery")
        recovery = str(mandate.get("allowed_autonomous_action") or mandate.get("primary_action") or "restore the revenue loop")
    elif deadline_at is not None:
        health = "waiting_for_proof_deadline"
        reason = "proof deadline has not arrived"
        recovery = "stay out and let the approved autonomous action run"
    else:
        health = "watching_money_proof"
        reason = str(mandate.get("primary_action") or "watch the current money proof")
        recovery = str(mandate.get("allowed_autonomous_action") or mandate.get("primary_action") or "continue the current money proof")

    return {
        "state": health,
        "reason": reason,
        "proof_state": state,
        "proof_deadline": deadline,
        "seconds_until_deadline": seconds_until_deadline,
        "expected_active_sends": expected_active_sends,
        "actual_active_sends": actual_active_sends,
        "autonomous_recovery_action": recovery,
        "owner_interrupt": health in {"paid_fulfillment_open", "execution_proof_missed", "recovery_required", "buyer_signal_open"},
        "do_not_judge_demand": health in {"execution_proof_missed", "recovery_required"},
    }


def _run_outbound_experiment_review_if_needed(bottleneck: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    if bottleneck != "outbound_targeting_or_copy":
        return {"status": "skipped", "summary": "outbound experiment review not needed for this bottleneck"}

    failure_sample = _experiment_failure_sample()
    sends = int(snapshot.get("outreach", {}).get("sends") or 0)
    replies = int(snapshot.get("outreach", {}).get("replies") or 0)
    payments = int(snapshot.get("money", {}).get("payments") or 0)

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
    measurable_sends = max(active_sends, sends)
    if measurable_sends < failure_sample:
        return {
            "status": "skipped",
            "summary": "active experiment still needs its own measurable send sample",
            "active_variant": active_variant,
            "active_experiment_sends": active_sends,
            "active_experiment_replies": active_replies,
            "aggregate_sends": sends,
            "aggregate_replies": replies,
            "aggregate_payments": payments,
            "measurable_sends": measurable_sends,
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
            "aggregate_replies": replies,
            "aggregate_payments": payments,
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
        "aggregate_replies": replies,
        "aggregate_payments": payments,
        "failure_sample": failure_sample,
        "experiment_variant": plan.get("experiment_variant"),
        "experiment_label": plan.get("experiment_label"),
        "decision_reasons": plan.get("decision_reasons", []),
        "daily_cap_recommendation": plan.get("daily_cap_recommendation"),
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _conversion_action_summary(actions: dict[str, Any]) -> dict[str, Any]:
    sent_by_action: dict[str, int] = {}
    failures_by_action: dict[str, int] = {}

    def walk(name: str, value: Any) -> None:
        if not isinstance(value, dict):
            return
        sent_count = _safe_int(value.get("sent_count"))
        if sent_count > 0:
            sent_by_action[name] = sent_by_action.get(name, 0) + sent_count
        failures = value.get("failures")
        if isinstance(failures, list) and failures:
            failures_by_action[name] = failures_by_action.get(name, 0) + len(failures)
        for child_name, child_value in value.items():
            if isinstance(child_value, dict):
                walk(f"{name}.{child_name}", child_value)

    for action_name, action_result in actions.items():
        walk(action_name, action_result)

    return {
        "sent_count": sum(sent_by_action.values()),
        "failure_count": sum(failures_by_action.values()),
        "sent_by_action": sent_by_action,
        "failures_by_action": failures_by_action,
    }


def run_relay_success_control_tick() -> dict[str, Any]:
    before = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(before)
    before_money_proof_mandate = _money_proof_mandate(before, bottleneck)
    before_money_proof_health = _money_proof_health(before_money_proof_mandate)

    actions: dict[str, Any] = {}
    actions["intake_smoke_check"] = _run_intake_smoke_check_if_needed()
    actions["delivery_smoke_check"] = _run_delivery_smoke_check_if_needed()
    actions["inbound_conversion"] = run_inbound_conversion_sweep()
    actions["paid_intake_reminders"] = run_paid_intake_reminder_sweep(
        hours=int(os.getenv("OPS_INTAKE_REMINDER_HOURS", "12") or "12")
    )
    actions["post_delivery_upsell"] = run_post_delivery_upsell_sweep(
        hours=int(os.getenv("OPS_UPSELL_DELAY_HOURS", "24") or "24")
    )
    actions["outbound_experiment_review"] = _run_outbound_experiment_review_if_needed(bottleneck, before)
    conversion_actions = _conversion_action_summary(actions)

    after = relay_success_snapshot(days=7)
    after_bottleneck = _bottleneck(after)
    money_proof_mandate = _money_proof_mandate(after, after_bottleneck)
    money_proof_health = _money_proof_health(money_proof_mandate)
    result = {
        "status": "ok",
        "bottleneck": bottleneck,
        "next_action": _next_action(bottleneck),
        "after_bottleneck": after_bottleneck,
        "after_next_action": _next_action(after_bottleneck),
        "before_money_proof_mandate": before_money_proof_mandate,
        "before_money_proof_health": before_money_proof_health,
        "money_proof_mandate": money_proof_mandate,
        "money_proof_health": money_proof_health,
        "bottleneck_changed": after_bottleneck != bottleneck,
        "before": before,
        "actions": actions,
        "conversion_actions": conversion_actions,
        "after": after,
        "created_at": _now().isoformat(),
    }

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type=SUCCESS_TICK_EVENT,
                prospect_external_id="relay-success",
                summary=f"{money_proof_mandate.get('state')}: {money_proof_mandate.get('primary_action')}",
                payload_json=json.dumps(result, ensure_ascii=False),
            )
        )
        session.commit()

    return result


def relay_success_status() -> dict[str, Any]:
    snapshot = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(snapshot)
    money_proof_mandate = _money_proof_mandate(snapshot, bottleneck)
    money_proof_health = _money_proof_health(money_proof_mandate)
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
        "money_proof_mandate": money_proof_mandate,
        "money_proof_health": money_proof_health,
        "snapshot": snapshot,
        "latest_tick": {
            "created_at": latest.created_at.isoformat(),
            "summary": latest.summary,
            "payload": _safe_json(latest.payload_json),
        }
        if latest
        else None,
    }
