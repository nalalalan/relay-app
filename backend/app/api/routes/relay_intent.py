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


def _latest_lead(db) -> dict[str, Any] | None:
    lead = db.query(RelayIntentLead).order_by(RelayIntentLead.created_at.desc()).first()
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
    if not recent.get("last_tally_event"):
        return "needs_intake_live_test"
    if not recent.get("last_delivery_or_sent_action"):
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

        sample_email = (
            _send_sample_email(email)
            if "sample" in source.lower()
            else {"status": "skipped", "reason": "not a sample request"}
        )
        try:
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
                            "sample_email": sample_email,
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            db.commit()
        except Exception:
            db.rollback()

        return {"ok": True, "lead_id": lead.id, "session_id": sid, "score": score, "sample_email": sample_email}
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

        lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).scalar() or 0
        hot_lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).filter(RelayIntentLead.score >= 50).scalar() or 0

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
            "last_production_transition": _latest_production_transition(db),
            "last_delivery_or_sent_action": _latest_action(db, "delivery") or _latest_action(db, "send") or _latest_action(db, "email"),
            "last_relay_intent_lead": _latest_lead(db),
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
        }
        checks["verdict"] = _ready_label(checks)
        return checks
    finally:
        db.close()
