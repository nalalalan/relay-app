import json
import os
import re
import uuid
from datetime import datetime, timedelta
from html import escape
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func

from app.core.config import settings
from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.models.production_wiring import (
    ProductionAction,
    ProductionException,
    ProductionLead,
    ProductionOpportunity,
    ProductionTransition,
)
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead


router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _relay_url(path: str = "") -> str:
    base = (settings.landing_page_url or "https://relay.aolabs.io").rstrip("/")
    clean_path = path if path.startswith("/") else f"/{path}" if path else ""
    return f"{base}{clean_path}"


class RelayIntentEventIn(BaseModel):
    session_id: str | None = None
    event_type: str = Field(..., min_length=1, max_length=80)
    path: str | None = None
    page_url: str | None = None
    target_text: str | None = None
    target_href: str | None = None
    referrer: str | None = None
    metadata: dict[str, Any] | None = None


class RelayIntentLeadIn(BaseModel):
    session_id: str | None = None
    email: str = Field(..., min_length=3, max_length=320)
    source: str | None = "sample_request"
    page_url: str | None = None
    referrer: str | None = None
    metadata: dict[str, Any] | None = None


def _session_id(value: str | None) -> str:
    clean = (value or "").strip()
    return clean[:128] if clean else str(uuid.uuid4())


def _json(data: dict[str, Any] | None) -> str | None:
    if not data:
        return None
    return json.dumps(data, ensure_ascii=False, sort_keys=True)[:8000]


def _safe_payload(raw: str | None) -> dict[str, Any]:
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


def _sample_email_html(to_email: str) -> str:
    sample_url = _relay_url("/sample.pdf")
    relay_url = _relay_url()
    checkout_url = settings.packet_checkout_url or "#"
    safe_email = escape(to_email)
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:620px">
      <p>Here is the Relay sample packet:</p>
      <p><a href="{sample_url}" style="color:#a05f2f;font-weight:700">Open the Relay sample PDF</a></p>
      <p>
        Relay turns rough call notes into client-ready follow-through:
        recap, next steps, follow-up, and CRM update.
      </p>
      <p>
        If you want the live one-call test, start here:
        <a href="{checkout_url}" style="color:#a05f2f;font-weight:700">Start the $40 relay</a>
      </p>
      <p style="font-size:13px;color:#756961">
        Sent to {safe_email} from <a href="{relay_url}" style="color:#756961">relay.aolabs.io</a>.
      </p>
    </div>
    """.strip()


def _send_sample_email(to_email: str) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=to_email,
            subject="Relay sample packet",
            html=_sample_email_html(to_email),
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=settings.reply_to_email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _send_messy_notes_email(payload: RelayIntentLeadIn, email: str, score: int) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    metadata = payload.metadata or {}
    notes = str(metadata.get("notes") or "").strip()[:5000]
    note_length = metadata.get("note_length") or len(notes)
    page_url = payload.page_url or _relay_url()
    safe_email = escape(email)
    safe_notes = escape(notes or "(No notes captured.)").replace("\n", "<br>")
    safe_page = escape(page_url)

    html = f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:720px">
      <p><strong>New Relay messy-notes submission</strong></p>
      <p><strong>Email:</strong> {safe_email}<br>
      <strong>Score:</strong> {score}<br>
      <strong>Note length:</strong> {escape(str(note_length))}</p>
      <div style="padding:14px 16px;border:1px solid #ead3c8;border-radius:12px;background:#fffaf6">
        {safe_notes}
      </div>
      <p style="font-size:13px;color:#756961">
        Source page: <a href="{safe_page}" style="color:#756961">{safe_page}</a>
      </p>
    </div>
    """.strip()

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=settings.reply_to_email,
            subject=f"New Relay notes from {email}",
            html=html,
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _messy_notes_customer_email_html(to_email: str) -> str:
    sample_url = _relay_url("/sample.pdf")
    notes_url = _relay_url("/#send-notes")
    checkout_url = settings.packet_checkout_url or "#"
    safe_email = escape(to_email)
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:620px">
      <p>Got your Relay notes.</p>
      <p>
        The paid next step is the one-call packet. I turn one messy sales or client call
        into the recap, next steps, follow-up draft, open questions, and CRM-ready update.
      </p>
      <p>
        <a href="{checkout_url}" style="color:#a05f2f;font-weight:700">Start the $40 packet</a>
      </p>
      <p>
        Sample:
        <a href="{sample_url}" style="color:#a05f2f;font-weight:700">open the sample packet</a>
      </p>
      <p style="font-size:13px;color:#756961">
        If you need to resend or add detail, use <a href="{notes_url}" style="color:#756961">the notes form</a>.
        Sent to {safe_email}.
      </p>
    </div>
    """.strip()


def _send_messy_notes_customer_email(email: str) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=email,
            subject="Got your Relay notes",
            html=_messy_notes_customer_email_html(email),
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=settings.reply_to_email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _upsert_relay_acquisition_prospect(
    db,
    lead: RelayIntentLead,
    payload: RelayIntentLeadIn,
    email: str,
    source: str,
    score: int,
) -> dict[str, Any]:
    metadata = payload.metadata or {}
    source_lower = (source or "").lower()
    if "checkout_intent" in source_lower:
        inbound_stage = "checkout_intent"
        minimum_score = 90
    elif "messy_notes" in source_lower:
        inbound_stage = "messy_notes"
        minimum_score = 85
    elif "sample" in source_lower:
        inbound_stage = "sample_requested"
        minimum_score = 60
    else:
        inbound_stage = "inbound_lead"
        minimum_score = 40

    effective_score = max(int(score or 0), minimum_score)
    notes = str(metadata.get("notes") or "").strip()[:5000]
    prospect = (
        db.query(AcquisitionProspect)
        .filter(AcquisitionProspect.contact_email == email)
        .order_by(AcquisitionProspect.created_at.desc())
        .first()
    )

    created = False
    if prospect is None:
        prospect = AcquisitionProspect(
            external_id=f"relay-lead:{lead.id}",
            contact_email=email,
            company_name=str(metadata.get("company_name") or metadata.get("client_name") or "Relay lead")[:255],
            source="relay_intent",
            status="interested",
            fit_score=effective_score,
            fit_band="inbound",
            last_reply_state=inbound_stage,
        )
        db.add(prospect)
        created = True

    locked_paid_state = prospect.status in {"paid", "intake_received"} or prospect.stripe_status == "paid"
    if not locked_paid_state:
        prospect.status = "interested"
    prospect.source = prospect.source or "relay_intent"
    prospect.fit_score = max(int(prospect.fit_score or 0), effective_score)
    prospect.fit_band = prospect.fit_band or "inbound"
    if not locked_paid_state:
        prospect.last_reply_state = inbound_stage
    prospect.notes = notes or prospect.notes
    prospect.payload_json = json.dumps(
        {
            "relay_lead_id": lead.id,
            "session_id": lead.session_id,
            "source": source,
            "score": effective_score,
            "raw_score": score,
            "inbound_stage": inbound_stage,
            "metadata": metadata,
        },
        ensure_ascii=False,
    )[:8000]

    return {
        "status": "created" if created else "updated",
        "external_id": prospect.external_id,
        "contact_email": prospect.contact_email,
        "inbound_stage": inbound_stage,
        "fit_score": prospect.fit_score,
    }


def _user_agent(request: Request) -> str | None:
    ua = request.headers.get("user-agent")
    return ua[:1000] if ua else None


def _lead_score(session_id: str, source: str | None) -> int:
    db = SessionLocal()
    try:
        recent = datetime.utcnow() - timedelta(days=14)
        events = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.session_id == session_id)
            .filter(RelayIntentEvent.created_at >= recent)
            .group_by(RelayIntentEvent.event_type)
            .all()
        )
        counts = {name: int(count) for name, count in events}
        score = 10
        score += counts.get("sample_click", 0) * 8
        score += counts.get("checkout_click", 0) * 15
        score += counts.get("pricing_click", 0) * 12
        score += counts.get("client_gate_click", 0) * 10
        if source:
            score += 5
        return min(score, 100)
    finally:
        db.close()


def _latest_acquisition_event(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(AcquisitionEvent)
    for term in event_terms:
        query = query.filter(AcquisitionEvent.event_type.ilike(f"%{term}%"))
    event = query.order_by(AcquisitionEvent.created_at.desc()).first()
    if not event:
        return None
    return {
        "event_type": event.event_type,
        "summary": event.summary,
        "prospect_external_id": event.prospect_external_id,
        "created_at": event.created_at.isoformat(),
    }


def _latest_acquisition_payload(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(AcquisitionEvent)
    for term in event_terms:
        query = query.filter(AcquisitionEvent.event_type.ilike(f"%{term}%"))
    event = query.order_by(AcquisitionEvent.created_at.desc()).first()
    if not event:
        return None
    return _safe_payload(event.payload_json)


def _compact_money_loop_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    refill = payload.get("refill_result") if isinstance(payload.get("refill_result"), dict) else {}
    fallback = refill.get("fallback_result") if isinstance(refill.get("fallback_result"), dict) else {}
    post_refill_outreach = (
        payload.get("post_refill_outreach_result")
        if isinstance(payload.get("post_refill_outreach_result"), dict)
        else {}
    )
    status_after = payload.get("status_after") if isinstance(payload.get("status_after"), dict) else {}
    refill_backoff = (
        payload.get("refill_timeout_backoff")
        if isinstance(payload.get("refill_timeout_backoff"), dict)
        else {}
    )
    return {
        "refill_status": refill.get("status"),
        "refill_reason": refill.get("reason"),
        "refill_error_type": refill.get("error_type"),
        "refill_http_status": refill.get("http_status"),
        "refill_timeout_seconds": refill.get("timeout_seconds"),
        "apollo_primary_status": refill.get("apollo_primary_status"),
        "apollo_fallback_status": refill.get("apollo_fallback_status"),
        "refill_error_present": bool(refill.get("error")),
        "refill_source": refill.get("source"),
        "refill_query": refill.get("q_keywords"),
        "refill_searched": refill.get("searched"),
        "refill_enriched_with_email": refill.get("enriched_with_email"),
        "refill_missing_email_after_enrichment": refill.get("missing_email_after_enrichment"),
        "refill_skipped_missing_email": refill.get("skipped_missing_email"),
        "refill_upserted": refill.get("upserted"),
        "refill_prospects_with_email": refill.get("prospects_with_email"),
        "refill_sendable_upserted": refill.get("sendable_upserted"),
        "refill_direct_sendable_upserted": refill.get("direct_sendable_upserted"),
        "refill_generic_sendable_upserted": refill.get("generic_sendable_upserted"),
        "refill_missing_email_count": refill.get("missing_email_count"),
        "refill_rejected_or_unsendable_count": refill.get("rejected_or_unsendable_count"),
        "refill_capacity_mode": refill.get("refill_capacity_mode"),
        "refill_capacity_before": refill.get("refill_capacity_before"),
        "refill_capacity_after": refill.get("refill_capacity_after"),
        "refill_capacity_delta": refill.get("refill_capacity_delta"),
        "apollo_endpoint": refill.get("apollo_endpoint"),
        "apollo_primary_error_status": refill.get("apollo_primary_error_status"),
        "apollo_enrich_errors": refill.get("enrich_errors"),
        "refill_attempts": refill.get("attempts"),
        "fallback_status": fallback.get("status"),
        "fallback_error_type": fallback.get("error_type"),
        "fallback_timeout_seconds": fallback.get("timeout_seconds"),
        "fallback_upserted": fallback.get("upserted"),
        "fallback_sendable_upserted": fallback.get("sendable_upserted"),
        "fallback_direct_sendable_upserted": fallback.get("direct_sendable_upserted"),
        "fallback_generic_sendable_upserted": fallback.get("generic_sendable_upserted"),
        "fallback_capacity_delta": fallback.get("refill_capacity_delta"),
        "fallback_attempts": refill.get("fallback_attempts"),
        "fallback_searched": fallback.get("searched"),
        "active_experiment_needs_sample": payload.get("active_experiment_needs_sample"),
        "active_experiment_new_due_before": payload.get("active_experiment_new_due_before"),
        "refill_due_before": payload.get("refill_due_before"),
        "refill_due_for_decision": payload.get("refill_due_for_decision"),
        "refill_backoff_active": refill_backoff.get("active"),
        "refill_backoff_reason": refill_backoff.get("timeout_reason") or refill_backoff.get("reason"),
        "refill_backoff_age_seconds": refill_backoff.get("age_seconds"),
        "refill_backoff_remaining_seconds": refill_backoff.get("remaining_seconds"),
        "direct_due_before": payload.get("direct_due_before"),
        "send_window_open_before": payload.get("send_window_open_before"),
        "outreach_phase": payload.get("outreach_phase"),
        "post_refill_outreach_sent": post_refill_outreach.get("send_result", {}).get("sent_count")
        if isinstance(post_refill_outreach.get("send_result"), dict)
        else None,
        "post_refill_outreach_summary": post_refill_outreach.get("send_result", {}).get("summary")
        if isinstance(post_refill_outreach.get("send_result"), dict)
        else None,
        "status_after": {
            "active_experiment_variant": status_after.get("active_experiment_variant"),
            "active_experiment_sends": status_after.get("active_experiment_sends"),
            "active_experiment_sample_target": status_after.get("active_experiment_sample_target"),
            "active_experiment_needs_sample": status_after.get("active_experiment_needs_sample"),
            "active_experiment_new_due_count": status_after.get("active_experiment_new_due_count"),
            "active_experiment_direct_new_due_count": status_after.get("active_experiment_direct_new_due_count"),
            "active_experiment_generic_new_due_count": status_after.get("active_experiment_generic_new_due_count"),
            "active_experiment_allowed_generic_new_due_count": status_after.get("active_experiment_allowed_generic_new_due_count"),
            "active_experiment_generic_sample_daily_cap": status_after.get("active_experiment_generic_sample_daily_cap"),
            "direct_due_count": status_after.get("direct_due_count"),
            "cap_remaining": status_after.get("cap_remaining"),
            "next_money_move": status_after.get("next_money_move"),
        },
    }


def _current_money_loop_runtime() -> dict[str, Any]:
    try:
        from app.services import relay_recovery_patch

        state = dict(getattr(relay_recovery_patch, "_money_loop_state", {}) or {})
    except Exception as exc:
        return {"status": "error", "summary": f"money_loop_state_unavailable:{type(exc).__name__}"}

    last_result = state.get("last_result") if isinstance(state.get("last_result"), dict) else None
    manual_result = state.get("last_manual_result") if isinstance(state.get("last_manual_result"), dict) else None
    running_seconds = None
    last_tick_at = state.get("last_tick_at") or ""
    if state.get("running") and last_tick_at:
        try:
            started = datetime.fromisoformat(str(last_tick_at).replace("Z", "+00:00"))
            if started.tzinfo is not None:
                started = started.astimezone().replace(tzinfo=None)
            running_seconds = max(int((datetime.now() - started).total_seconds()), 0)
        except Exception:
            running_seconds = None
    return {
        "enabled": bool(state.get("enabled")),
        "running": bool(state.get("running")),
        "last_tick_at": last_tick_at,
        "running_seconds": running_seconds,
        "last_error": state.get("last_error") or "",
        "next_sleep_seconds": state.get("next_sleep_seconds"),
        "next_wake_reason": state.get("next_wake_reason") or "",
        "ticks": int(state.get("ticks") or 0),
        "last_manual_kick_at": state.get("last_manual_kick_at") or "",
        "last_result": _compact_money_loop_payload(last_result),
        "last_manual_result": _compact_money_loop_payload(manual_result),
    }


def _latest_production_transition(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(ProductionTransition)
    for term in event_terms:
        query = query.filter(ProductionTransition.event_type.ilike(f"%{term}%"))
    event = query.order_by(ProductionTransition.created_at.desc()).first()
    if not event:
        return None
    return {
        "event_type": event.event_type,
        "summary": event.summary,
        "entity_external_id": event.entity_external_id,
        "old_state": event.old_state,
        "new_state": event.new_state,
        "created_at": event.created_at.isoformat(),
    }


def _latest_action(db, *action_terms: str) -> dict[str, Any] | None:
    query = db.query(ProductionAction)
    for term in action_terms:
        query = query.filter(ProductionAction.action_type.ilike(f"%{term}%"))
    action = query.order_by(ProductionAction.created_at.desc()).first()
    if not action:
        return None
    return {
        "action_type": action.action_type,
        "status": action.status,
        "to_email": action.to_email,
        "subject": action.subject,
        "created_at": action.created_at.isoformat(),
        "updated_at": action.updated_at.isoformat() if action.updated_at else None,
    }


def _latest_lead(
    db,
    source_term: str | None = None,
    *,
    exclude_internal: bool = False,
) -> dict[str, Any] | None:
    query = db.query(RelayIntentLead)
    if source_term:
        query = query.filter(RelayIntentLead.source.ilike(f"%{source_term}%"))
    if exclude_internal:
        query = query.filter(RelayIntentLead.email.notin_(_internal_emails()))

    lead = query.order_by(RelayIntentLead.created_at.desc()).first()
    if not lead:
        return None
    return {
        "email": lead.email,
        "source": lead.source,
        "score": lead.score,
        "session_id": lead.session_id,
        "created_at": lead.created_at.isoformat(),
    }


def _env_present(name: str) -> bool:
    return bool(str(os.getenv(name, "")).strip())


def _ready_label(checks: dict[str, Any]) -> str:
    missing = []
    env = checks.get("env", {})
    if not env.get("DATABASE_URL"):
        missing.append("DATABASE_URL")
    if not env.get("RESEND_API_KEY"):
        missing.append("RESEND_API_KEY")
    if not env.get("STRIPE_WEBHOOK_SECRET"):
        missing.append("STRIPE_WEBHOOK_SECRET")
    if not env.get("TALLY_WEBHOOK_SECRET"):
        missing.append("TALLY_WEBHOOK_SECRET")
    if not env.get("PACKET_CHECKOUT_URL"):
        missing.append("PACKET_CHECKOUT_URL")

    if missing:
        return "needs_env: " + ", ".join(missing)

    recent = checks.get("recent", {})
    if not recent.get("last_stripe_event"):
        return "needs_stripe_live_test"
    if not recent.get("last_tally_event") and not recent.get("last_paid_relay_notes_fulfillment"):
        if recent.get("last_real_notes_intake_lead") or recent.get("last_notes_intake_lead"):
            return "notes_intake_ready_needs_paid_buyer"
        return "needs_intake_live_test"
    if not recent.get("last_delivery_or_sent_action") and not recent.get("last_paid_relay_notes_fulfillment"):
        return "needs_delivery_live_test"
    return "ready_or_nearly_ready"


@router.post("/event")
def record_relay_event(payload: RelayIntentEventIn, request: Request) -> dict[str, Any]:
    sid = _session_id(payload.session_id)
    event_type = payload.event_type.strip().lower().replace(" ", "_")[:80]

    db = SessionLocal()
    try:
        event = RelayIntentEvent(
            session_id=sid,
            event_type=event_type,
            path=(payload.path or "")[:512] or None,
            page_url=(payload.page_url or "")[:1000] or None,
            target_text=(payload.target_text or "")[:500] or None,
            target_href=(payload.target_href or "")[:1000] or None,
            referrer=(payload.referrer or "")[:1000] or None,
            user_agent=_user_agent(request),
            metadata_json=_json(payload.metadata),
        )
        db.add(event)
        db.commit()
        db.refresh(event)
        return {"ok": True, "event_id": event.id, "session_id": sid}
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"could not record relay event: {exc}") from exc
    finally:
        db.close()


@router.post("/lead")
def record_relay_lead(payload: RelayIntentLeadIn, request: Request) -> dict[str, Any]:
    sid = _session_id(payload.session_id)
    email = payload.email.strip().lower()

    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="valid email required")

    source = (payload.source or "sample_request").strip()[:120]
    score = _lead_score(sid, source)

    db = SessionLocal()
    try:
        lead = RelayIntentLead(
            session_id=sid,
            email=email,
            source=source,
            page_url=(payload.page_url or "")[:1000] or None,
            referrer=(payload.referrer or "")[:1000] or None,
            user_agent=_user_agent(request),
            score=score,
            metadata_json=_json(payload.metadata),
        )
        db.add(lead)

        db.add(
            RelayIntentEvent(
                session_id=sid,
                event_type="lead_capture",
                path=None,
                page_url=(payload.page_url or "")[:1000] or None,
                target_text=email,
                target_href=None,
                referrer=(payload.referrer or "")[:1000] or None,
                user_agent=_user_agent(request),
                metadata_json=_json({"source": source, "score": score}),
            )
        )

        db.commit()
        db.refresh(lead)

        source_lower = source.lower()
        acquisition_prospect = (
            _upsert_relay_acquisition_prospect(db, lead, payload, email, source, score)
            if any(term in source_lower for term in ["messy_notes", "sample", "checkout_intent"]) and not _is_internal_email(email)
            else {
                "status": "skipped",
                "reason": "internal test email" if _is_internal_email(email) else "not an inbound buyer signal",
            }
        )
        db.commit()

        sample_email = (
            _send_sample_email(email)
            if "sample" in source_lower
            else {"status": "skipped", "reason": "not a sample request"}
        )
        operator_email = (
            _send_messy_notes_email(payload, email, score)
            if "messy_notes" in source_lower
            else {"status": "skipped", "reason": "not a messy notes request"}
        )
        customer_email = (
            _send_messy_notes_customer_email(email)
            if "messy_notes" in source_lower
            else {"status": "skipped", "reason": "not a messy notes request"}
        )
        try:
            if acquisition_prospect.get("status") in {"created", "updated"}:
                db.add(
                    AcquisitionEvent(
                        event_type=f"relay_inbound_prospect_{acquisition_prospect.get('status', 'unknown')}",
                        prospect_external_id=str(acquisition_prospect.get("external_id") or f"relay-lead:{lead.id}"),
                        summary=(
                            f"inbound {acquisition_prospect.get('inbound_stage', 'lead')} prospect "
                            f"{acquisition_prospect.get('status', 'unknown')} for {email}"
                        ),
                        payload_json=json.dumps(
                            {
                                "lead_id": lead.id,
                                "session_id": sid,
                                "email": email,
                                "source": source,
                                "score": score,
                                "acquisition_prospect": acquisition_prospect,
                            },
                            ensure_ascii=False,
                        ),
                    )
                )
            db.add(
                AcquisitionEvent(
                    event_type=f"relay_sample_email_{sample_email.get('status', 'unknown')}",
                    prospect_external_id=f"relay-lead:{lead.id}",
                    summary=f"sample email {sample_email.get('status', 'unknown')} for {email}",
                    payload_json=json.dumps(
                        {
                            "lead_id": lead.id,
                            "session_id": sid,
                            "email": email,
                            "source": source,
                            "score": score,
                            "acquisition_prospect": acquisition_prospect,
                            "sample_email": sample_email,
                            "operator_email": operator_email,
                            "customer_email": customer_email,
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            if "messy_notes" in source_lower:
                db.add(
                    AcquisitionEvent(
                        event_type=f"relay_messy_notes_email_{operator_email.get('status', 'unknown')}",
                        prospect_external_id=f"relay-lead:{lead.id}",
                        summary=f"messy notes email {operator_email.get('status', 'unknown')} for {email}",
                        payload_json=json.dumps(
                            {
                                "lead_id": lead.id,
                                "session_id": sid,
                                "email": email,
                                "source": source,
                                "score": score,
                                "acquisition_prospect": acquisition_prospect,
                                "operator_email": operator_email,
                                "customer_email": customer_email,
                            },
                            ensure_ascii=False,
                        ),
                    )
                )
            db.commit()
        except Exception:
            db.rollback()

        return {
            "ok": True,
            "lead_id": lead.id,
            "session_id": sid,
            "score": score,
            "sample_email": sample_email,
            "operator_email": operator_email,
            "customer_email": customer_email,
            "acquisition_prospect": acquisition_prospect,
        }
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"could not record relay lead: {exc}") from exc
    finally:
        db.close()


@router.get("/intent-summary")
def relay_intent_summary(days: int = 7, limit: int = 20) -> dict[str, Any]:
    days = max(1, min(days, 90))
    limit = max(1, min(limit, 100))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        event_counts = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.event_type)
            .order_by(func.count(RelayIntentEvent.id).desc())
            .all()
        )

        leads = (
            db.query(RelayIntentLead)
            .filter(RelayIntentLead.created_at >= since)
            .order_by(RelayIntentLead.created_at.desc())
            .limit(limit)
            .all()
        )

        hot_sessions = (
            db.query(RelayIntentEvent.session_id, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.session_id)
            .order_by(func.count(RelayIntentEvent.id).desc())
            .limit(limit)
            .all()
        )

        return {
            "ok": True,
            "days": days,
            "event_counts": [{"event_type": name, "count": int(count)} for name, count in event_counts],
            "leads": [
                {
                    "email": lead.email,
                    "source": lead.source,
                    "score": lead.score,
                    "created_at": lead.created_at.isoformat(),
                    "session_id": lead.session_id,
                }
                for lead in leads
            ],
            "hot_sessions": [
                {"session_id": session_id, "event_count": int(count)}
                for session_id, count in hot_sessions
            ],
        }
    finally:
        db.close()


@router.get("/ops-check")
def relay_ops_check(days: int = 14) -> dict[str, Any]:
    days = max(1, min(days, 90))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        env = {
            "DATABASE_URL": _env_present("DATABASE_URL"),
            "RESEND_API_KEY": _env_present("RESEND_API_KEY"),
            "STRIPE_SECRET_KEY": _env_present("STRIPE_SECRET_KEY"),
            "STRIPE_WEBHOOK_SECRET": _env_present("STRIPE_WEBHOOK_SECRET"),
            "TALLY_WEBHOOK_SECRET": _env_present("TALLY_WEBHOOK_SECRET"),
            "PACKET_CHECKOUT_URL": bool(getattr(settings, "packet_checkout_url", "") or os.getenv("PACKET_CHECKOUT_URL", "")),
            "CLIENT_INTAKE_DESTINATION": bool(getattr(settings, "client_intake_destination", "") or os.getenv("CLIENT_INTAKE_DESTINATION", "")),
            "FROM_EMAIL_FULFILLMENT": bool(getattr(settings, "from_email_fulfillment", "") or os.getenv("FROM_EMAIL_FULFILLMENT", "")),
        }

        route_surface = {
            "stripe_webhook_route": "/webhooks/stripe",
            "tally_webhook_route": "/webhooks/tally",
            "client_gate_route": "/client-gate/redeem",
            "production_event_route": "/production/event",
            "production_digest_route": "/production/digest",
            "autopilot_batch_route": "/autopilot/batch",
            "autopilot_digest_route": "/autopilot/digest",
            "intent_summary_route": "/api/relay/intent-summary",
        }

        event_counts = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.event_type)
            .all()
        )
        intent_counts = {name: int(count) for name, count in event_counts}
        internal_sessions = [
            session_id
            for (session_id,) in (
                db.query(RelayIntentLead.session_id)
                .filter(RelayIntentLead.created_at >= since)
                .filter(RelayIntentLead.email.in_(_internal_emails()))
                .all()
            )
            if session_id
        ]
        real_event_query = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
        )
        if internal_sessions:
            real_event_query = real_event_query.filter(RelayIntentEvent.session_id.notin_(internal_sessions))
        real_event_counts = real_event_query.group_by(RelayIntentEvent.event_type).all()
        real_intent_counts = {name: int(count) for name, count in real_event_counts}

        lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).scalar() or 0
        hot_lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).filter(RelayIntentLead.score >= 50).scalar() or 0
        real_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.notin_(_internal_emails()))
            .scalar()
            or 0
        )
        real_hot_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.notin_(_internal_emails()))
            .filter(RelayIntentLead.score >= 50)
            .scalar()
            or 0
        )
        internal_test_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.in_(_internal_emails()))
            .scalar()
            or 0
        )

        prospect_counts = (
            db.query(AcquisitionProspect.status, func.count(AcquisitionProspect.id))
            .group_by(AcquisitionProspect.status)
            .all()
        )
        stripe_paid_count = db.query(func.count(AcquisitionProspect.id)).filter(AcquisitionProspect.stripe_status == "paid").scalar() or 0
        intake_received_count = db.query(func.count(AcquisitionProspect.id)).filter(AcquisitionProspect.intake_status == "received").scalar() or 0

        production_lead_count = db.query(func.count(ProductionLead.id)).scalar() or 0
        production_opportunity_count = db.query(func.count(ProductionOpportunity.id)).scalar() or 0
        open_exception_count = db.query(func.count(ProductionException.id)).filter(ProductionException.resolved == False).scalar() or 0  # noqa: E712
        pending_action_count = db.query(func.count(ProductionAction.id)).filter(ProductionAction.status == "pending").scalar() or 0
        sent_action_count = db.query(func.count(ProductionAction.id)).filter(ProductionAction.status.in_(["sent", "completed", "done"])).scalar() or 0

        recent = {
            "last_stripe_event": _latest_acquisition_event(db, "stripe"),
            "last_tally_event": _latest_acquisition_event(db, "intake"),
            "last_payment_or_paid_prospect": _latest_acquisition_event(db, "paid"),
            "last_success_control_tick": _latest_acquisition_event(db, "relay_success_control_tick"),
            "last_money_loop_tick": _latest_acquisition_event(db, "relay_money_loop_tick"),
            "last_money_loop_detail": _compact_money_loop_payload(_latest_acquisition_payload(db, "relay_money_loop_tick")),
            "last_outbound_experiment_plan": _latest_acquisition_event(db, "relay_experiment_plan"),
            "last_inbound_followup": _latest_acquisition_event(db, "autopilot_messy_notes_checkout_followup_sent")
            or _latest_acquisition_event(db, "autopilot_messy_notes_second_followup_sent")
            or _latest_acquisition_event(db, "autopilot_sample_notes_followup_sent")
            or _latest_acquisition_event(db, "autopilot_sample_second_followup_sent")
            or _latest_acquisition_event(db, "autopilot_checkout_intent_followup_sent")
            or _latest_acquisition_event(db, "autopilot_checkout_intent_second_followup_sent"),
            "last_inbound_prospect": _latest_acquisition_event(db, "relay_inbound_prospect"),
            "last_paid_relay_notes_fulfillment": _latest_acquisition_event(db, "autopilot_paid_relay_notes_fulfilled"),
            "last_production_transition": _latest_production_transition(db),
            "last_delivery_or_sent_action": _latest_action(db, "delivery") or _latest_action(db, "send") or _latest_action(db, "email"),
            "last_relay_intent_lead": _latest_lead(db),
            "last_notes_intake_lead": _latest_lead(db, "messy_notes"),
            "last_real_notes_intake_lead": _latest_lead(db, "messy_notes", exclude_internal=True),
            "last_checkout_intent_lead": _latest_lead(db, "checkout_intent", exclude_internal=True),
        }

        checks = {
            "ok": True,
            "days": days,
            "route_surface": route_surface,
            "env": env,
            "intent": {
                "event_counts": intent_counts,
                "lead_count": int(lead_count),
                "hot_lead_count": int(hot_lead_count),
                "real_event_counts": real_intent_counts,
                "real_lead_count": int(real_lead_count),
                "real_hot_lead_count": int(real_hot_lead_count),
                "internal_test_lead_count": int(internal_test_lead_count),
            },
            "acquisition": {
                "status_counts": {status or "unknown": int(count) for status, count in prospect_counts},
                "stripe_paid_count": int(stripe_paid_count),
                "intake_received_count": int(intake_received_count),
            },
            "production": {
                "lead_count": int(production_lead_count),
                "opportunity_count": int(production_opportunity_count),
                "pending_action_count": int(pending_action_count),
                "sent_action_count": int(sent_action_count),
                "open_exception_count": int(open_exception_count),
            },
            "recent": recent,
            "money_loop_runtime": _current_money_loop_runtime(),
        }
        try:
            from app.services.relay_performance import relay_performance_status
            from app.services.relay_success_controller import relay_success_status
            from app.services.custom_outreach import outreach_status

            performance = relay_performance_status()
            success = relay_success_status()
            active_experiment = performance.get("active_experiment") or {}
            outreach = outreach_status()
            checks["relay_performance"] = {
                "active_experiment": {
                    "experiment_variant": active_experiment.get("experiment_variant"),
                    "experiment_label": active_experiment.get("experiment_label"),
                    "source": active_experiment.get("source"),
                    "week_start_date": active_experiment.get("week_start_date"),
                },
                "active_experiment_signal": performance.get("active_experiment_signal") or {},
                "rolling_7_day": performance.get("rolling_7_day") or {},
                "active_experiment_queue": {
                    "active_experiment_variant": outreach.get("active_experiment_variant"),
                    "active_experiment_sends": outreach.get("active_experiment_sends"),
                    "active_experiment_sample_target": outreach.get("active_experiment_sample_target"),
                    "active_experiment_needs_sample": outreach.get("active_experiment_needs_sample"),
                    "active_experiment_new_due_count": outreach.get("active_experiment_new_due_count"),
                    "active_experiment_direct_new_due_count": outreach.get("active_experiment_direct_new_due_count"),
                    "active_experiment_generic_new_due_count": outreach.get("active_experiment_generic_new_due_count"),
                    "active_experiment_allowed_generic_new_due_count": outreach.get("active_experiment_allowed_generic_new_due_count"),
                    "active_experiment_generic_sample_daily_cap": outreach.get("active_experiment_generic_sample_daily_cap"),
                    "direct_due_count": outreach.get("direct_due_count"),
                    "cap_remaining": outreach.get("cap_remaining"),
                    "effective_daily_cap": outreach.get("effective_daily_cap"),
                    "send_window_is_open": outreach.get("send_window_is_open"),
                    "send_window_reason": outreach.get("send_window_reason"),
                    "send_window_next_open_local": outreach.get("send_window_next_open_local"),
                    "send_window_seconds_until_open": outreach.get("send_window_seconds_until_open"),
                    "send_window_business_days_only": outreach.get("send_window_business_days_only"),
                    "next_money_move": outreach.get("next_money_move"),
                },
            }
            success_snapshot = success.get("snapshot") or {}
            checks["relay_success"] = {
                "bottleneck": success.get("bottleneck"),
                "next_action": success.get("next_action"),
                "money": success_snapshot.get("money") or {},
                "intent": success_snapshot.get("intent") or {},
                "outreach": success_snapshot.get("outreach") or {},
                "conversion": success_snapshot.get("conversion") or {},
            }
        except Exception as exc:
            checks["relay_performance"] = {
                "status": "error",
                "summary": str(exc),
            }
        checks["verdict"] = _ready_label(checks)
        return checks
    finally:
        db.close()
