from __future__ import annotations

from dataclasses import asdict, dataclass
from html import escape
import json
import os
from typing import Any, Callable, Dict, List, Literal

from sqlalchemy import select

from app.core.config import entry_checkout_url, settings
from sqlalchemy.orm import Session

from app.db.base import SessionLocal
from app.integrations.resend_client import ResendClient
from app.models.production_wiring import (
    ProductionAction,
    ProductionException,
    ProductionLead,
    ProductionOpportunity,
    ProductionTransition,
)
from app.services.acquisition_engine_v1 import run_acquisition_engine_v1
from app.services.close_path_v1 import decide_close_path
from app.services.outreach_sender_pipeline_state_v1 import ingest_reply, send_or_queue_candidate
from app.services.proposal_audit_launcher_v1 import run_proposal_audit_launcher_v1

EventType = Literal[
    "lead_found",
    "outreach_sent",
    "reply_received",
    "proposal_sent",
    "payment_received",
    "fulfillment_sent",
]


@dataclass
class ProductionEventDecision:
    event_type: str
    entity_id: str
    status: Literal["processed", "ignored", "error"]
    summary: str
    actions_created: List[Dict[str, Any]]
    exceptions_created: List[Dict[str, Any]]
    lead_state: str = ""
    close_state: str = ""
    fulfillment_state: str = ""


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _text_to_html(text: str) -> str:
    return "<pre style='font-family:Arial,Helvetica,sans-serif;white-space:pre-wrap;line-height:1.5;'>" + escape(text) + "</pre>"


def _session_factory(sf: Callable[[], Session] | None = None) -> Callable[[], Session]:
    return sf or SessionLocal


def _transition_exists(session: Session, event_id: str) -> bool:
    if not event_id:
        return False
    stmt = select(ProductionTransition).where(ProductionTransition.event_id == event_id)
    return session.execute(stmt).scalar_one_or_none() is not None


def _get_lead(session: Session, lead_id: str) -> ProductionLead | None:
    stmt = select(ProductionLead).where(ProductionLead.external_id == lead_id)
    return session.execute(stmt).scalar_one_or_none()


def _ensure_lead(session: Session, lead_id: str) -> ProductionLead:
    lead = _get_lead(session, lead_id)
    if lead is None:
        lead = ProductionLead(external_id=lead_id)
        session.add(lead)
        session.flush()
    return lead


def _get_opportunity(session: Session, opp_id: str) -> ProductionOpportunity | None:
    stmt = select(ProductionOpportunity).where(ProductionOpportunity.external_id == opp_id)
    return session.execute(stmt).scalar_one_or_none()


def _ensure_opportunity(session: Session, opp_id: str, lead_id: str) -> ProductionOpportunity:
    opp = _get_opportunity(session, opp_id)
    if opp is None:
        opp = ProductionOpportunity(external_id=opp_id, lead_external_id=lead_id)
        session.add(opp)
        session.flush()
    return opp


def _append_transition(session: Session, event_id: str, entity_id: str, event_type: str, old_state: str, new_state: str, summary: str) -> Dict[str, Any]:
    t = ProductionTransition(
        event_id=event_id or "",
        entity_external_id=entity_id,
        event_type=event_type,
        old_state=old_state,
        new_state=new_state,
        summary=summary,
    )
    session.add(t)
    return {
        "event_id": t.event_id,
        "entity_external_id": entity_id,
        "event_type": event_type,
        "old_state": old_state,
        "new_state": new_state,
        "summary": summary,
    }


def _append_exception(session: Session, entity_id: str, exception_type: str, summary: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    item = ProductionException(
        entity_external_id=entity_id,
        exception_type=exception_type,
        summary=summary,
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
    )
    session.add(item)
    return {
        "entity_external_id": entity_id,
        "exception_type": exception_type,
        "summary": summary,
    }


def _existing_dedupe_keys(session: Session) -> set[str]:
    stmt = select(ProductionAction.dedupe_key).where(ProductionAction.dedupe_key != "")
    rows = session.execute(stmt).scalars().all()
    return set(rows)


def _append_action(
    session: Session,
    entity_type: str,
    entity_id: str,
    action_type: str,
    status: str,
    dedupe_key: str = "",
    to_email: str = "",
    subject: str = "",
    body: str = "",
    payload: Dict[str, Any] | None = None,
    provider_message_id: str = "",
) -> Dict[str, Any]:
    item = ProductionAction(
        entity_type=entity_type,
        entity_external_id=entity_id,
        action_type=action_type,
        status=status,
        dedupe_key=dedupe_key,
        to_email=to_email,
        subject=subject,
        body=body,
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
        provider_message_id=provider_message_id,
    )
    session.add(item)
    return {
        "entity_type": entity_type,
        "entity_external_id": entity_id,
        "action_type": action_type,
        "status": status,
        "dedupe_key": dedupe_key,
        "to_email": to_email,
        "subject": subject,
    }


def _maybe_send_email(to_email: str, subject: str, body: str, auto_send: bool) -> tuple[str, str]:
    if not auto_send:
        return "pending_send", ""
    result = ResendClient().send_outbound_email(to_email=to_email, subject=subject, html=_text_to_html(body))
    provider_id = ""
    if isinstance(result, dict):
        provider_id = str(result.get("id", ""))
    return "sent", provider_id


def _build_outreach_candidate_from_lead_found(decision: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "lead_id": decision["lead_id"],
        "company_name": decision["company_name"],
        "contact_name": payload.get("contact_name", ""),
        "contact_email": payload.get("contact_email", ""),
        "fit_band": decision["fit_band"],
        "route": decision["route"],
        "outreach_subject": decision["outreach_subject"],
        "outreach_body": decision["outreach_body"],
        "founder_digest_line": decision["founder_digest_line"],
        "next_action": decision["next_action"],
        "source": payload.get("source", ""),
        "state": "new",
    }


def _payment_link_for_offer(offer_name: str) -> str:
    return entry_checkout_url()


def _build_opportunity_payload_from_lead(lead: ProductionLead, reply_state: str, reply_text: str) -> Dict[str, Any]:
    return {
        "opportunity_id": f"opp-{lead.external_id}",
        "company_name": lead.company_name,
        "contact_name": lead.contact_name,
        "contact_email": lead.contact_email,
        "fit_band": lead.fit_band,
        "route": lead.route,
        "vertical": lead.vertical,
        "estimated_monthly_call_volume": int(lead.estimated_monthly_call_volume) if str(lead.estimated_monthly_call_volume).isdigit() else None,
        "known_pain": lead.notes,
        "notes": lead.notes,
        "reply_state": reply_state,
        "reply_text": reply_text,
    }


def process_production_event(
    event: Dict[str, Any],
    session_factory: Callable[[], Session] | None = None,
    auto_send: bool | None = None,
) -> ProductionEventDecision:
    sf = _session_factory(session_factory)
    auto_send = _bool_env("PRODUCTION_WIRING_AUTO_SEND", False) if auto_send is None else auto_send

    event_id = str(event.get("event_id", "")).strip()
    event_type = str(event.get("event_type", "")).strip()
    payload = event.get("payload") or {}

    with sf() as session:
        if _transition_exists(session, event_id):
            return ProductionEventDecision(
                event_type=event_type,
                entity_id=str(payload.get("lead_id") or payload.get("id") or ""),
                status="ignored",
                summary="duplicate event ignored",
                actions_created=[],
                exceptions_created=[],
            )

        actions: List[Dict[str, Any]] = []
        exceptions: List[Dict[str, Any]] = []

        try:
            if event_type == "lead_found":
                decision = asdict(run_acquisition_engine_v1(payload))
                lead_id = decision["lead_id"]
                lead = _ensure_lead(session, lead_id)
                old_lead_state = lead.lead_state

                lead.company_name = decision["company_name"]
                lead.contact_name = str(payload.get("contact_name", "")).strip()
                lead.contact_email = str(payload.get("contact_email", "")).strip()
                lead.website = str(payload.get("website", "")).strip()
                lead.vertical = str(payload.get("vertical", "")).strip()
                lead.source = str(payload.get("source", "")).strip()
                lead.estimated_monthly_call_volume = str(payload.get("estimated_monthly_call_volume", "")).strip()
                lead.fit_band = decision["fit_band"]
                lead.fit_score = int(decision["score"])
                lead.route = decision["route"]
                lead.notes = str(payload.get("notes", "")).strip()
                lead.lead_state = "qualified" if decision["fit_band"] != "discard" else "discarded"

                candidate = _build_outreach_candidate_from_lead_found(decision, payload)
                send_result = asdict(send_or_queue_candidate(candidate, existing_dedupe_keys=_existing_dedupe_keys(session)))
                lead.pipeline_state = send_result["pipeline_state"]

                if send_result["send_status"] == "sent":
                    status, provider_id = _maybe_send_email(
                        to_email=lead.contact_email,
                        subject=send_result["subject"],
                        body=decision["outreach_body"],
                        auto_send=auto_send,
                    )
                    actions.append(
                        _append_action(
                            session,
                            entity_type="lead",
                            entity_id=lead_id,
                            action_type="send_outreach",
                            status=status,
                            dedupe_key=send_result["dedupe_key"],
                            to_email=lead.contact_email,
                            subject=send_result["subject"],
                            body=decision["outreach_body"],
                            payload={"fit_band": decision["fit_band"], "route": decision["route"]},
                            provider_message_id=provider_id,
                        )
                    )
                elif send_result["send_status"] == "queued":
                    actions.append(
                        _append_action(
                            session,
                            entity_type="lead",
                            entity_id=lead_id,
                            action_type="nurture_queue",
                            status="queued",
                            dedupe_key=send_result["dedupe_key"],
                            to_email=lead.contact_email,
                            subject=send_result["subject"],
                            body=decision["outreach_body"],
                            payload={"reason": send_result["reason"]},
                        )
                    )
                elif send_result["send_status"] == "duplicate_blocked":
                    exceptions.append(_append_exception(session, lead_id, "duplicate_outreach", "duplicate outreach blocked"))

                _append_transition(
                    session,
                    event_id=event_id,
                    entity_id=lead_id,
                    event_type=event_type,
                    old_state=old_lead_state,
                    new_state=lead.lead_state,
                    summary=f"{decision['fit_band']} -> {send_result['send_status']}",
                )
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead_id,
                    status="processed",
                    summary=f"{lead.company_name} classified as {decision['fit_band']} and {send_result['send_status']}",
                    actions_created=actions,
                    exceptions_created=exceptions,
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            if event_type == "outreach_sent":
                lead_id = str(payload.get("lead_id", "")).strip()
                lead = _ensure_lead(session, lead_id)
                old = lead.pipeline_state
                lead.pipeline_state = "sent"
                _append_transition(session, event_id, lead_id, event_type, old, "sent", "outreach confirmed sent")
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead_id,
                    status="processed",
                    summary="outreach marked sent",
                    actions_created=[],
                    exceptions_created=[],
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            if event_type == "reply_received":
                lead_id = str(payload.get("lead_id", "")).strip()
                reply_text = str(payload.get("reply_text", "")).strip()
                lead = _get_lead(session, lead_id)
                if lead is None:
                    exceptions.append(_append_exception(session, lead_id, "missing_lead", "reply received for unknown lead", payload))
                    session.commit()
                    return ProductionEventDecision(
                        event_type=event_type,
                        entity_id=lead_id,
                        status="error",
                        summary="reply received for unknown lead",
                        actions_created=[],
                        exceptions_created=exceptions,
                    )

                transition = asdict(ingest_reply(lead_id, lead.pipeline_state or "sent", reply_text))
                old_pipeline = lead.pipeline_state
                lead.pipeline_state = transition["new_state"]

                close_state_map = {
                    "replied_interested": "interested",
                    "replied_needs_info": "needs_info",
                    "replied_not_now": "not_now",
                    "replied_not_fit": "closed_lost",
                }
                if transition["new_state"] in close_state_map:
                    lead.close_state = close_state_map[transition["new_state"]]

                opp_payload = _build_opportunity_payload_from_lead(lead, transition["new_state"], reply_text)
                launcher = asdict(run_proposal_audit_launcher_v1(opp_payload))
                opp = _ensure_opportunity(session, launcher["opportunity_id"], lead.external_id)
                opp.company_name = launcher["company_name"]
                opp.launch_type = launcher["launch_type"]
                opp.recommended_offer = launcher["recommended_offer"]
                opp.price_guidance = launcher["recommended_price_guidance"]
                opp.best_next_commercial_move = launcher["best_next_commercial_move"]
                opp.buyer_email_subject = launcher["buyer_email_subject"]
                opp.buyer_email_body = launcher["buyer_email_body"]

                close_payload = {
                    **launcher,
                    "contact_name": lead.contact_name,
                    "contact_email": lead.contact_email,
                    "reply_state": transition["new_state"],
                    "reply_text": reply_text,
                    "payment_link": _payment_link_for_offer(launcher["recommended_offer"]),
                    "current_close_state": lead.close_state,
                }
                close_decision = asdict(decide_close_path(close_payload))
                lead.close_state = close_decision["next_close_state"]
                opp.close_route = close_decision["close_route"]
                opp.opportunity_state = close_decision["next_close_state"]

                if close_decision["close_route"] in {"send_proposal", "send_clarify"}:
                    status, provider_id = _maybe_send_email(
                        to_email=lead.contact_email,
                        subject=close_decision["subject"],
                        body=close_decision["body"],
                        auto_send=auto_send,
                    )
                    actions.append(
                        _append_action(
                            session,
                            entity_type="opportunity",
                            entity_id=opp.external_id,
                            action_type="send_close_email",
                            status=status,
                            to_email=lead.contact_email,
                            subject=close_decision["subject"],
                            body=close_decision["body"],
                            payload={"close_route": close_decision["close_route"], "launch_type": launcher["launch_type"]},
                            provider_message_id=provider_id,
                        )
                    )

                if transition["new_state"] == "replied_needs_info":
                    exceptions.append(
                        _append_exception(session, opp.external_id, "needs_info", "reply needs clarification before clean close")
                    )

                _append_transition(
                    session,
                    event_id=event_id,
                    entity_id=lead.external_id,
                    event_type=event_type,
                    old_state=old_pipeline,
                    new_state=lead.pipeline_state,
                    summary=f"{transition['reply_classification']} -> {close_decision['close_route']}",
                )
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead.external_id,
                    status="processed",
                    summary=f"reply classified as {transition['reply_classification']} and routed to {close_decision['close_route']}",
                    actions_created=actions,
                    exceptions_created=exceptions,
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            if event_type == "proposal_sent":
                lead_id = str(payload.get("lead_id", "")).strip()
                lead = _ensure_lead(session, lead_id)
                old = lead.close_state
                lead.close_state = "payment_pending"
                _append_transition(session, event_id, lead_id, event_type, old, "payment_pending", "proposal sent")
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead_id,
                    status="processed",
                    summary="proposal marked sent",
                    actions_created=[],
                    exceptions_created=[],
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            if event_type == "payment_received":
                lead_id = str(payload.get("lead_id", "")).strip()
                lead = _get_lead(session, lead_id)
                if lead is None:
                    exceptions.append(_append_exception(session, lead_id, "missing_lead", "payment received for unknown lead", payload))
                    session.commit()
                    return ProductionEventDecision(
                        event_type=event_type,
                        entity_id=lead_id,
                        status="error",
                        summary="payment received for unknown lead",
                        actions_created=[],
                        exceptions_created=exceptions,
                    )

                opp_id = f"opp-{lead.external_id}"
                opp = _get_opportunity(session, opp_id)
                if opp is None:
                    exceptions.append(_append_exception(session, lead_id, "missing_opportunity", "payment received without proposal/opportunity", payload))
                    session.commit()
                    return ProductionEventDecision(
                        event_type=event_type,
                        entity_id=lead_id,
                        status="error",
                        summary="payment received without matching opportunity",
                        actions_created=[],
                        exceptions_created=exceptions,
                    )

                close_payload = {
                    "opportunity_id": opp.external_id,
                    "company_name": lead.company_name,
                    "contact_name": lead.contact_name,
                    "contact_email": lead.contact_email,
                    "launch_type": opp.launch_type,
                    "recommended_offer": opp.recommended_offer,
                    "recommended_price_guidance": opp.price_guidance,
                    "best_next_commercial_move": opp.best_next_commercial_move,
                    "buyer_email_subject": opp.buyer_email_subject,
                    "buyer_email_body": opp.buyer_email_body,
                    "internal_operator_note": "",
                    "reply_state": "payment_received",
                    "reply_text": "Paid.",
                    "payment_link": str(payload.get("payment_link", "")) or _payment_link_for_offer(opp.recommended_offer),
                    "current_close_state": lead.close_state,
                }
                close_decision = asdict(decide_close_path(close_payload))

                old_close = lead.close_state
                lead.close_state = "paid"
                lead.fulfillment_state = close_decision["next_close_state"]
                opp.opportunity_state = "paid"

                actions.append(
                    _append_action(
                        session,
                        entity_type="opportunity",
                        entity_id=opp.external_id,
                        action_type="queue_fulfillment",
                        status="queued",
                        payload={
                            "company_name": lead.company_name,
                            "recommended_offer": opp.recommended_offer,
                            "reason": close_decision["fulfillment_trigger_reason"],
                        },
                    )
                )
                exceptions.append(
                    _append_exception(session, opp.external_id, "paid_opportunity", f"{lead.company_name} paid; fulfillment queued")
                )

                _append_transition(session, event_id, lead.external_id, event_type, old_close, "paid", "payment received")
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead.external_id,
                    status="processed",
                    summary="payment received and fulfillment queued",
                    actions_created=actions,
                    exceptions_created=exceptions,
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            if event_type == "fulfillment_sent":
                lead_id = str(payload.get("lead_id", "")).strip()
                lead = _ensure_lead(session, lead_id)
                old = lead.fulfillment_state
                lead.fulfillment_state = "sent"
                _append_transition(session, event_id, lead_id, event_type, old, "sent", "fulfillment delivered")
                session.commit()
                return ProductionEventDecision(
                    event_type=event_type,
                    entity_id=lead_id,
                    status="processed",
                    summary="fulfillment marked sent",
                    actions_created=[],
                    exceptions_created=[],
                    lead_state=lead.lead_state,
                    close_state=lead.close_state,
                    fulfillment_state=lead.fulfillment_state,
                )

            exceptions.append(_append_exception(session, "", "unsupported_event", f"unsupported event_type: {event_type}", event))
            session.commit()
            return ProductionEventDecision(
                event_type=event_type,
                entity_id="",
                status="error",
                summary=f"unsupported event_type: {event_type}",
                actions_created=[],
                exceptions_created=exceptions,
            )

        except Exception as exc:
            entity_id = str(payload.get("lead_id") or payload.get("id") or "")
            exceptions.append(_append_exception(session, entity_id, "processing_error", str(exc), payload))
            session.commit()
            return ProductionEventDecision(
                event_type=event_type,
                entity_id=entity_id,
                status="error",
                summary=str(exc),
                actions_created=actions,
                exceptions_created=exceptions,
            )


def production_digest(session_factory: Callable[[], Session] | None = None) -> Dict[str, Any]:
    sf = _session_factory(session_factory)
    with sf() as session:
        leads = session.execute(select(ProductionLead)).scalars().all()
        actions = session.execute(select(ProductionAction)).scalars().all()
        exceptions = session.execute(select(ProductionException).where(ProductionException.resolved == False)).scalars().all()  # noqa: E712
        return {
            "lead_count": len(leads),
            "action_count": len(actions),
            "open_exception_count": len(exceptions),
            "paid_or_queued": [
                {"company_name": lead.company_name, "close_state": lead.close_state, "fulfillment_state": lead.fulfillment_state}
                for lead in leads
                if lead.close_state in {"paid", "payment_pending"} or lead.fulfillment_state in {"fulfillment_queued", "sent"}
            ],
            "exceptions": [
                {"entity_external_id": item.entity_external_id, "exception_type": item.exception_type, "summary": item.summary}
                for item in exceptions
            ],
        }
