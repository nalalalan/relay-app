import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func

from app.db.base import SessionLocal
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead


router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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
        return {"ok": True, "lead_id": lead.id, "session_id": sid, "score": score}
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
