from __future__ import annotations

import html
import hashlib
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.resend_client import ResendClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead


def _session() -> Session:
    return SessionLocal()


def _internal_emails() -> set[str]:
    configured = os.getenv("RELAY_INTERNAL_EMAILS", "pham.alann@gmail.com").split(",")
    return {email.strip().lower() for email in configured if email.strip()}


def _is_internal_email(email: str | None) -> bool:
    return (email or "").strip().lower() in _internal_emails()


def _intake_url() -> str:
    return (
        settings.client_intake_destination
        or os.getenv("CLIENT_INTAKE_URL", "").strip()
        or _notes_url()
    )


def _notes_url() -> str:
    return settings.landing_page_url.rstrip("/") + "/#send-notes"


def _pack_url() -> str:
    return (
        os.getenv("PACKET_5_PACK_URL", "").strip()
        or settings.packet_checkout_url
    )


def _monthly_url() -> str:
    return (
        os.getenv("MONTHLY_AUTOPILOT_URL", "").strip()
        or settings.landing_page_url
    )


def _find_prospect_by_email(session: Session, email: str) -> AcquisitionProspect | None:
    stmt = (
        select(AcquisitionProspect)
        .where(AcquisitionProspect.contact_email == email)
        .order_by(AcquisitionProspect.created_at.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _paid_for_email(session: Session, email: str) -> bool:
    if _is_internal_email(email):
        return False
    prospect = _find_prospect_by_email(session, email)
    if prospect is not None and prospect.stripe_status == "paid":
        return True

    rows = session.execute(
        select(AcquisitionEvent.payload_json)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(100)
    ).scalars().all()
    for raw in rows:
        payload = _safe_json(raw)
        blob = json.dumps(payload, ensure_ascii=False).lower()
        if email and email.lower() in blob:
            return True
    return False


def _ensure_paid_prospect(session: Session, email: str) -> AcquisitionProspect:
    prospect = _find_prospect_by_email(session, email)
    if prospect is None:
        prospect = AcquisitionProspect(
            external_id=f"stripe-buyer:{hashlib.sha256(email.encode('utf-8')).hexdigest()[:24]}",
            contact_email=email,
            company_name="paid Relay buyer",
            source="stripe",
            status="paid",
            stripe_status="paid",
            fit_score=100,
            fit_band="paid",
        )
        session.add(prospect)
    else:
        prospect.status = "paid"
        prospect.stripe_status = "paid"
    return prospect


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _latest_relay_notes_lead(session: Session, email: str) -> RelayIntentLead | None:
    stmt = (
        select(RelayIntentLead)
        .where(RelayIntentLead.email == email)
        .where(RelayIntentLead.source.ilike("%messy_notes%"))
        .order_by(RelayIntentLead.created_at.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _relay_notes_tally_payload(lead: RelayIntentLead, email: str) -> dict[str, Any] | None:
    metadata = _safe_json(lead.metadata_json)
    notes = str(metadata.get("notes") or "").strip()
    if not notes:
        return None

    client_name = str(
        metadata.get("client_name")
        or metadata.get("company_name")
        or metadata.get("client")
        or "Client call"
    ).strip()
    focus = str(
        metadata.get("focus")
        or "recap, next steps, follow-up draft, open questions, and CRM-ready update"
    ).strip()
    tone = str(metadata.get("tone") or "clear and professional").strip()

    def field(label: str, value: str) -> dict[str, str]:
        return {"label": label, "value": value}

    return {
        "data": {
            "submissionId": f"relay-notes-{lead.id}",
            "respondentId": f"relay-lead-{lead.id}",
            "createdAt": lead.created_at.isoformat() if lead.created_at else datetime.utcnow().isoformat(),
            "fields": [
                field("Where should we send your handoff packet?", email),
                field("Client or company name", client_name),
                field("What should this focus on?", focus),
                field("Preferred tone for the follow-up email", tone),
                field("Paste your rough client call notes", notes),
            ],
        }
    }


def _fulfill_paid_relay_notes(session: Session, prospect: AcquisitionProspect, email: str) -> dict[str, Any]:
    if _event_exists(session, prospect.external_id, "autopilot_paid_relay_notes_fulfilled"):
        return {"status": "skipped", "summary": "relay notes already fulfilled"}

    lead = _latest_relay_notes_lead(session, email)
    if lead is None:
        return {"status": "skipped", "summary": "no relay notes found for buyer"}

    payload = _relay_notes_tally_payload(lead, email)
    if payload is None:
        return {"status": "skipped", "summary": "relay notes were empty"}

    try:
        from app.workers.fulfillment import process_tally_submission

        result = process_tally_submission(payload)
        sent_count = int((result.get("sending") or {}).get("sent_count") or 0)
        status = "sent" if sent_count > 0 else "processed"
        _log_event(
            session,
            "autopilot_paid_relay_notes_fulfilled",
            prospect.external_id,
            "fulfilled paid buyer from existing Relay messy notes",
            {
                "email": email,
                "relay_lead_id": lead.id,
                "submission_id": result.get("submission_id", ""),
                "result": result,
            },
        )
        session.commit()
        return {
            "status": status,
            "summary": "fulfilled paid buyer from existing Relay notes",
            "relay_lead_id": lead.id,
            "result": result,
        }
    except Exception as exc:
        _log_event(
            session,
            "autopilot_paid_relay_notes_failed",
            prospect.external_id,
            "paid Relay notes fulfillment failed",
            {"email": email, "relay_lead_id": lead.id, "error": str(exc)[:1000]},
        )
        session.commit()
        return {"status": "error", "summary": "relay notes fulfillment failed", "error": str(exc)[:500]}


def _event_exists(session: Session, prospect_external_id: str, event_type: str) -> bool:
    stmt = (
        select(AcquisitionEvent.id)
        .where(AcquisitionEvent.prospect_external_id == prospect_external_id)
        .where(AcquisitionEvent.event_type == event_type)
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none() is not None


def _log_event(
    session: Session,
    event_type: str,
    prospect_external_id: str,
    summary: str,
    payload: Dict[str, Any] | None = None,
) -> None:
    session.add(
        AcquisitionEvent(
            event_type=event_type,
            prospect_external_id=prospect_external_id,
            summary=summary,
            payload_json=(__import__("json").dumps(payload or {}, ensure_ascii=False)),
        )
    )


def _send_html_email(to_email: str, subject: str, blocks: list[str]) -> dict[str, Any]:
    body = "<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;color:#1f1f1f;line-height:1.6;'>" + "".join(blocks) + "</div>"
    client = ResendClient()
    return client.send_email(
        to_email=to_email,
        subject=subject,
        html=body,
        from_email=settings.from_email_fulfillment or settings.from_email_outbound,
        reply_to=settings.reply_to_email,
    )


def _p(text: str) -> str:
    return f"<p style='margin:0 0 16px 0'>{html.escape(text)}</p>"


def _a(label: str, url: str) -> str:
    return (
        "<p style='margin:0 0 16px 0'>"
        f"<a href='{html.escape(url, quote=True)}' style='color:#a05f2f;font-weight:700'>"
        f"{html.escape(label)}</a></p>"
    )


def _send_conversion_email(
    *,
    to_email: str,
    subject: str,
    blocks: list[str],
    event_type: str,
    prospect_external_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    send_result = _send_html_email(to_email, subject, blocks)
    with _session() as session:
        _log_event(
            session,
            event_type,
            prospect_external_id,
            f"sent {event_type}",
            {"email": to_email, "send_result": send_result, **payload},
        )
        session.commit()
    return {"status": "sent", "send_result": send_result}


def send_paid_onboarding_for_email(email: str) -> dict[str, Any]:
    email = (email or "").strip().lower()
    if not email:
        return {"status": "ignored", "summary": "missing email"}

    with _session() as session:
        prospect = _ensure_paid_prospect(session, email)
        session.commit()

        notes_fulfillment = _fulfill_paid_relay_notes(session, prospect, email)
        if notes_fulfillment.get("status") in {"sent", "processed"}:
            return notes_fulfillment

        if _event_exists(session, prospect.external_id, "autopilot_paid_onboarding_sent"):
            return {"status": "skipped", "summary": "already sent"}

        intake_url = _intake_url()
        if not intake_url:
            return {"status": "ignored", "summary": "missing intake url"}

        company = prospect.company_name or "your team"
        subject = "You're in - send the call"
        blocks = [
            _p(f"Thanks - you're in for {company}."),
            _p("Next step is just sending the details for the first real call."),
            _p(f"Intake: {intake_url}"),
            _p("Once that comes through, the packet gets built and sent back automatically."),
            _p("- Alan"),
        ]
        send_result = _send_html_email(email, subject, blocks)
        _log_event(
            session,
            "autopilot_paid_onboarding_sent",
            prospect.external_id,
            "sent paid onboarding email",
            {"email": email, "send_result": send_result},
        )
        session.commit()
        return {"status": "sent", "summary": "paid onboarding sent", "send_result": send_result}


def send_intake_ack_for_email(email: str) -> dict[str, Any]:
    email = (email or "").strip().lower()
    if not email:
        return {"status": "ignored", "summary": "missing email"}

    with _session() as session:
        prospect = _find_prospect_by_email(session, email)
        if prospect is None:
            return {"status": "ignored", "summary": "no matching prospect"}
        if _event_exists(session, prospect.external_id, "autopilot_intake_ack_sent"):
            return {"status": "skipped", "summary": "already sent"}

        subject = "Got it - packet is in motion"
        blocks = [
            _p("Got it."),
            _p("I have what I need from the intake and the packet is in motion."),
            _p("You'll get the finished recap, next steps, and follow-up draft automatically."),
            _p("- Alan"),
        ]
        send_result = _send_html_email(email, subject, blocks)
        _log_event(
            session,
            "autopilot_intake_ack_sent",
            prospect.external_id,
            "sent intake acknowledgement",
            {"email": email, "send_result": send_result},
        )
        session.commit()
        return {"status": "sent", "summary": "intake ack sent", "send_result": send_result}


def run_paid_intake_reminder_sweep(hours: int = 12) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0

    with _session() as session:
        paid_events = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "stripe_paid")
            .where(AcquisitionEvent.created_at <= cutoff)
            .order_by(AcquisitionEvent.created_at.asc())
        ).scalars().all()

        for event in paid_events:
            stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == event.prospect_external_id)
            prospect = session.execute(stmt).scalar_one_or_none()
            if prospect is None:
                skipped += 1
                continue

            if prospect.intake_status != "not_started":
                skipped += 1
                continue

            if _event_exists(session, prospect.external_id, "autopilot_intake_reminder_sent"):
                skipped += 1
                continue

            intake_url = _intake_url()
            if not intake_url or not prospect.contact_email:
                skipped += 1
                continue

            subject = "Quick reminder - send the call details"
            blocks = [
                _p("Quick reminder - I still need the call details to generate the packet."),
                _p(f"Intake: {intake_url}"),
                _p("Once that is in, the packet gets built automatically."),
                _p("- Alan"),
            ]
            send_result = _send_html_email(prospect.contact_email, subject, blocks)
            _log_event(
                session,
                "autopilot_intake_reminder_sent",
                prospect.external_id,
                "sent intake reminder",
                {"send_result": send_result},
            )
            sent_count += 1

        session.commit()

    return {"status": "ok", "sent_count": sent_count, "skipped": skipped, "hours": hours}


def run_post_delivery_upsell_sweep(hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0

    with _session() as session:
        intake_events = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "intake_received")
            .where(AcquisitionEvent.created_at <= cutoff)
            .order_by(AcquisitionEvent.created_at.asc())
        ).scalars().all()

        for event in intake_events:
            stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == event.prospect_external_id)
            prospect = session.execute(stmt).scalar_one_or_none()
            if prospect is None:
                skipped += 1
                continue

            if _event_exists(session, prospect.external_id, "autopilot_upsell_sent"):
                skipped += 1
                continue

            if not prospect.contact_email:
                skipped += 1
                continue

            subject = "Want this for more calls?"
            blocks = [
                _p("If you want this for more calls, easiest move is just to use the next-step links below."),
                _p(f"5-pack / next packet: {_pack_url()}"),
                _p(f"Monthly / site: {_monthly_url()}"),
                _p("If you do want the same flow running automatically for future calls, that's the path."),
                _p("- Alan"),
            ]
            send_result = _send_html_email(prospect.contact_email, subject, blocks)
            _log_event(
                session,
                "autopilot_upsell_sent",
                prospect.external_id,
                "sent upsell email",
                {"send_result": send_result},
            )
            sent_count += 1

        session.commit()

    return {"status": "ok", "sent_count": sent_count, "skipped": skipped, "hours": hours}


def run_messy_notes_checkout_followup_sweep(hours: int = 2) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        leads = session.execute(
            select(RelayIntentLead)
            .where(RelayIntentLead.source.ilike("%messy_notes%"))
            .where(RelayIntentLead.created_at <= cutoff)
            .order_by(RelayIntentLead.created_at.asc())
            .limit(50)
        ).scalars().all()

        send_candidates: list[RelayIntentLead] = []
        for lead in leads:
            email = (lead.email or "").strip().lower()
            external_id = f"relay-lead:{lead.id}"
            if not email:
                skipped += 1
                continue
            if _is_internal_email(email):
                skipped += 1
                continue
            if _paid_for_email(session, email):
                skipped += 1
                continue
            if _event_exists(session, external_id, "autopilot_messy_notes_checkout_followup_sent"):
                skipped += 1
                continue
            send_candidates.append(lead)

    for lead in send_candidates:
        try:
            external_id = f"relay-lead:{lead.id}"
            blocks = [
                _p("I have your messy notes."),
                _p(
                    "If you want me to turn that into the actual packet, the paid one-call test is the next step."
                ),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("I will turn it into the recap, next steps, follow-up draft, open questions, and CRM-ready update."),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Want me to turn this into the packet?",
                blocks=blocks,
                event_type="autopilot_messy_notes_checkout_followup_sent",
                prospect_external_id=external_id,
                payload={"relay_lead_id": lead.id, "hours": hours},
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def run_messy_notes_second_followup_sweep(hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        first_followups = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "autopilot_messy_notes_checkout_followup_sent")
            .where(AcquisitionEvent.created_at <= cutoff)
            .order_by(AcquisitionEvent.created_at.asc())
            .limit(100)
        ).scalars().all()

        send_candidates: list[RelayIntentLead] = []
        seen_leads: set[int] = set()
        for followup in first_followups:
            payload = _safe_json(followup.payload_json)
            lead_id = payload.get("relay_lead_id")
            if lead_id is None and followup.prospect_external_id.startswith("relay-lead:"):
                lead_id = followup.prospect_external_id.split(":", 1)[1].strip()
            try:
                lead_id_int = int(lead_id)
            except Exception:
                skipped += 1
                continue
            if lead_id_int in seen_leads:
                skipped += 1
                continue
            seen_leads.add(lead_id_int)

            external_id = f"relay-lead:{lead_id_int}"
            if _event_exists(session, external_id, "autopilot_messy_notes_second_followup_sent"):
                skipped += 1
                continue

            lead = session.execute(
                select(RelayIntentLead).where(RelayIntentLead.id == lead_id_int).limit(1)
            ).scalar_one_or_none()
            if lead is None or not lead.email:
                skipped += 1
                continue
            if _is_internal_email(lead.email):
                skipped += 1
                continue
            if _paid_for_email(session, lead.email):
                skipped += 1
                continue
            send_candidates.append(lead)

    for lead in send_candidates:
        try:
            external_id = f"relay-lead:{lead.id}"
            blocks = [
                _p("Closing the loop on your Relay notes."),
                _p("If you still want me to turn that rough call into the actual handoff packet, the $40 one-call test is the clean next step."),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("If you want to add or replace the rough notes first, send them here."),
                _a("Send messy notes", _notes_url()),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Should I still turn this into the packet?",
                blocks=blocks,
                event_type="autopilot_messy_notes_second_followup_sent",
                prospect_external_id=external_id,
                payload={"relay_lead_id": lead.id, "hours": hours},
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def run_sample_request_notes_followup_sweep(hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        leads = session.execute(
            select(RelayIntentLead)
            .where(RelayIntentLead.source.ilike("%sample%"))
            .where(RelayIntentLead.created_at <= cutoff)
            .order_by(RelayIntentLead.created_at.asc())
            .limit(50)
        ).scalars().all()

        send_candidates: list[RelayIntentLead] = []
        for lead in leads:
            email = (lead.email or "").strip().lower()
            external_id = f"relay-lead:{lead.id}"
            if not email:
                skipped += 1
                continue
            if _is_internal_email(email):
                skipped += 1
                continue
            if _paid_for_email(session, email):
                skipped += 1
                continue
            if _event_exists(session, external_id, "autopilot_sample_notes_followup_sent"):
                skipped += 1
                continue
            messy_notes = session.execute(
                select(RelayIntentLead.id)
                .where(RelayIntentLead.email == email)
                .where(RelayIntentLead.source.ilike("%messy_notes%"))
                .limit(1)
            ).scalar_one_or_none()
            if messy_notes is not None:
                skipped += 1
                continue
            send_candidates.append(lead)

    for lead in send_candidates:
        try:
            external_id = f"relay-lead:{lead.id}"
            blocks = [
                _p("You asked for the Relay sample."),
                _p("The easiest real test is one messy call note. Send rough bullets, a transcript, or an ugly notes dump."),
                _a("Send messy notes", _notes_url()),
                _p("If you already know you want the paid packet, the one-call test is $40."),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Have one messy call to test?",
                blocks=blocks,
                event_type="autopilot_sample_notes_followup_sent",
                prospect_external_id=external_id,
                payload={"relay_lead_id": lead.id, "hours": hours},
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def run_sample_request_second_followup_sweep(hours: int = 72) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        first_followups = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "autopilot_sample_notes_followup_sent")
            .where(AcquisitionEvent.created_at <= cutoff)
            .order_by(AcquisitionEvent.created_at.asc())
            .limit(100)
        ).scalars().all()

        send_candidates: list[RelayIntentLead] = []
        seen_leads: set[int] = set()
        for followup in first_followups:
            payload = _safe_json(followup.payload_json)
            lead_id = payload.get("relay_lead_id")
            if lead_id is None and followup.prospect_external_id.startswith("relay-lead:"):
                lead_id = followup.prospect_external_id.split(":", 1)[1].strip()
            try:
                lead_id_int = int(lead_id)
            except Exception:
                skipped += 1
                continue
            if lead_id_int in seen_leads:
                skipped += 1
                continue
            seen_leads.add(lead_id_int)

            external_id = f"relay-lead:{lead_id_int}"
            if _event_exists(session, external_id, "autopilot_sample_second_followup_sent"):
                skipped += 1
                continue

            lead = session.execute(
                select(RelayIntentLead).where(RelayIntentLead.id == lead_id_int).limit(1)
            ).scalar_one_or_none()
            if lead is None or not lead.email:
                skipped += 1
                continue
            email = (lead.email or "").strip().lower()
            if _is_internal_email(email):
                skipped += 1
                continue
            if _paid_for_email(session, email):
                skipped += 1
                continue
            messy_notes = session.execute(
                select(RelayIntentLead.id)
                .where(RelayIntentLead.email == email)
                .where(RelayIntentLead.source.ilike("%messy_notes%"))
                .limit(1)
            ).scalar_one_or_none()
            if messy_notes is not None:
                skipped += 1
                continue
            send_candidates.append(lead)

    for lead in send_candidates:
        try:
            external_id = f"relay-lead:{lead.id}"
            blocks = [
                _p("Checking once more after the Relay sample."),
                _p("The fastest useful test is still one rough call note. Send the messy version and Relay will keep the next step simple."),
                _a("Send messy notes", _notes_url()),
                _p("If you already know you want the paid packet, the one-call test is $40."),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Still have one call to test?",
                blocks=blocks,
                event_type="autopilot_sample_second_followup_sent",
                prospect_external_id=external_id,
                payload={"relay_lead_id": lead.id, "hours": hours},
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def _latest_lead_for_session(session: Session, session_id: str) -> RelayIntentLead | None:
    return session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.session_id == session_id)
        .order_by(RelayIntentLead.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def run_checkout_intent_followup_sweep(hours: int = 1) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        checkout_events = session.execute(
            select(RelayIntentEvent)
            .where(RelayIntentEvent.event_type == "checkout_click")
            .where(RelayIntentEvent.created_at <= cutoff)
            .order_by(RelayIntentEvent.created_at.asc())
            .limit(100)
        ).scalars().all()

        candidates: list[tuple[RelayIntentEvent | None, RelayIntentLead]] = []
        seen_sessions: set[str] = set()
        for event in checkout_events:
            session_id = (event.session_id or "").strip()
            if not session_id or session_id in seen_sessions:
                skipped += 1
                continue
            seen_sessions.add(session_id)

            external_id = f"relay-session:{session_id}"
            if _event_exists(session, external_id, "autopilot_checkout_intent_followup_sent"):
                skipped += 1
                continue

            lead = _latest_lead_for_session(session, session_id)
            if lead is None or not lead.email:
                skipped += 1
                continue
            if _is_internal_email(lead.email):
                skipped += 1
                continue
            if _paid_for_email(session, lead.email):
                skipped += 1
                continue
            candidates.append((event, lead))

        checkout_leads = session.execute(
            select(RelayIntentLead)
            .where(RelayIntentLead.source.ilike("%checkout_intent%"))
            .where(RelayIntentLead.created_at <= cutoff)
            .order_by(RelayIntentLead.created_at.asc())
            .limit(100)
        ).scalars().all()
        for lead in checkout_leads:
            session_id = (lead.session_id or "").strip()
            if not session_id or session_id in seen_sessions:
                skipped += 1
                continue
            seen_sessions.add(session_id)

            external_id = f"relay-session:{session_id}"
            if _event_exists(session, external_id, "autopilot_checkout_intent_followup_sent"):
                skipped += 1
                continue
            if not lead.email or _is_internal_email(lead.email) or _paid_for_email(session, lead.email):
                skipped += 1
                continue
            candidates.append((None, lead))

    for event, lead in candidates:
        try:
            session_id = ((event.session_id if event is not None else lead.session_id) or "").strip()
            external_id = f"relay-session:{session_id}"
            blocks = [
                _p("You opened the paid Relay packet path."),
                _p("If you still want one messy call turned into a finished recap, follow-up draft, CRM-ready update, and next-step checklist, the $40 packet is here."),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("If you want to add or resend rough notes first, use the notes form."),
                _a("Send messy notes", _notes_url()),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Still want the Relay packet?",
                blocks=blocks,
                event_type="autopilot_checkout_intent_followup_sent",
                prospect_external_id=external_id,
                payload={
                    "relay_event_id": event.id if event is not None else None,
                    "relay_lead_id": lead.id,
                    "session_id": session_id,
                    "hours": hours,
                },
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def run_checkout_intent_second_followup_sweep(hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    sent_count = 0
    skipped = 0
    failures: list[str] = []

    with _session() as session:
        first_followups = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "autopilot_checkout_intent_followup_sent")
            .where(AcquisitionEvent.created_at <= cutoff)
            .order_by(AcquisitionEvent.created_at.asc())
            .limit(100)
        ).scalars().all()

        candidates: list[tuple[AcquisitionEvent, RelayIntentLead]] = []
        seen_sessions: set[str] = set()
        for followup in first_followups:
            payload = _safe_json(followup.payload_json)
            session_id = str(payload.get("session_id") or "").strip()
            if not session_id and followup.prospect_external_id.startswith("relay-session:"):
                session_id = followup.prospect_external_id.split(":", 1)[1].strip()
            if not session_id or session_id in seen_sessions:
                skipped += 1
                continue
            seen_sessions.add(session_id)

            external_id = f"relay-session:{session_id}"
            if _event_exists(session, external_id, "autopilot_checkout_intent_second_followup_sent"):
                skipped += 1
                continue

            lead = _latest_lead_for_session(session, session_id)
            if lead is None or not lead.email:
                skipped += 1
                continue
            if _is_internal_email(lead.email):
                skipped += 1
                continue
            if _paid_for_email(session, lead.email):
                skipped += 1
                continue
            candidates.append((followup, lead))

    for followup, lead in candidates:
        try:
            payload = _safe_json(followup.payload_json)
            session_id = str(payload.get("session_id") or "").strip()
            if not session_id and followup.prospect_external_id.startswith("relay-session:"):
                session_id = followup.prospect_external_id.split(":", 1)[1].strip()
            external_id = f"relay-session:{session_id}"
            blocks = [
                _p("Closing the loop on the Relay packet."),
                _p("If you still want the one-call handoff, the $40 path is still the fastest way to get the recap, follow-up draft, CRM-ready update, and next-step checklist finished."),
                _a("Start the $40 packet", settings.packet_checkout_url),
                _p("If the call notes are not ready yet, send the rough version first and Relay will keep the next step simple."),
                _a("Send messy notes", _notes_url()),
                _p("- Alan"),
            ]
            _send_conversion_email(
                to_email=lead.email,
                subject="Should Relay still do this one?",
                blocks=blocks,
                event_type="autopilot_checkout_intent_second_followup_sent",
                prospect_external_id=external_id,
                payload={
                    "first_followup_event_id": followup.id,
                    "relay_lead_id": lead.id,
                    "session_id": session_id,
                    "hours": hours,
                },
            )
            sent_count += 1
        except Exception as exc:
            failures.append(str(exc)[:500])

    return {
        "status": "ok",
        "sent_count": sent_count,
        "skipped": skipped,
        "failures": failures,
        "hours": hours,
    }


def run_inbound_conversion_sweep() -> dict[str, Any]:
    messy_hours = int(os.getenv("RELAY_MESSY_NOTES_FOLLOWUP_HOURS", "2") or "2")
    messy_second_hours = int(os.getenv("RELAY_MESSY_NOTES_SECOND_FOLLOWUP_HOURS", "24") or "24")
    sample_hours = int(os.getenv("RELAY_SAMPLE_FOLLOWUP_HOURS", "24") or "24")
    sample_second_hours = int(os.getenv("RELAY_SAMPLE_SECOND_FOLLOWUP_HOURS", "72") or "72")
    checkout_hours = int(os.getenv("RELAY_CHECKOUT_FOLLOWUP_HOURS", "1") or "1")
    checkout_second_hours = int(os.getenv("RELAY_CHECKOUT_SECOND_FOLLOWUP_HOURS", "24") or "24")
    return {
        "messy_notes_followup": run_messy_notes_checkout_followup_sweep(hours=messy_hours),
        "messy_notes_second_followup": run_messy_notes_second_followup_sweep(hours=messy_second_hours),
        "sample_request_followup": run_sample_request_notes_followup_sweep(hours=sample_hours),
        "sample_request_second_followup": run_sample_request_second_followup_sweep(hours=sample_second_hours),
        "checkout_intent_followup": run_checkout_intent_followup_sweep(hours=checkout_hours),
        "checkout_intent_second_followup": run_checkout_intent_second_followup_sweep(hours=checkout_second_hours),
    }
