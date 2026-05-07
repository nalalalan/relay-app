from __future__ import annotations

from dataclasses import asdict, dataclass

from app.core.config import settings
from typing import Any, Dict, List, Literal
import re


CloseState = Literal[
    "new",
    "interested",
    "needs_info",
    "not_now",
    "not_fit",
    "proposal_sent",
    "payment_pending",
    "paid",
    "fulfillment_queued",
    "closed_lost",
]


@dataclass
class CloseOpportunity:
    opportunity_id: str
    company_name: str
    contact_name: str
    contact_email: str
    launch_type: str
    recommended_offer: str
    recommended_price_guidance: str
    best_next_commercial_move: str
    buyer_email_subject: str
    buyer_email_body: str
    internal_operator_note: str
    reply_state: str
    reply_text: str
    payment_link: str = ""
    current_close_state: CloseState = "new"


@dataclass
class ClosePathDecision:
    opportunity_id: str
    company_name: str
    current_close_state: CloseState
    next_close_state: CloseState
    close_route: Literal["send_proposal", "send_clarify", "wait", "close_lost", "trigger_fulfillment"]
    subject: str
    body: str
    payment_link_used: str
    fulfillment_should_trigger: bool
    fulfillment_trigger_reason: str
    next_action: str
    founder_digest_line: str
    state_record: Dict[str, Any]


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_close_opportunity(payload: Dict[str, Any]) -> CloseOpportunity:
    return CloseOpportunity(
        opportunity_id=_norm(payload.get("opportunity_id") or payload.get("lead_id") or payload.get("id")),
        company_name=_norm(payload.get("company_name")),
        contact_name=_norm(payload.get("contact_name")),
        contact_email=_norm(payload.get("contact_email") or payload.get("email")),
        launch_type=_norm(payload.get("launch_type")),
        recommended_offer=_norm(payload.get("recommended_offer")),
        recommended_price_guidance=_norm(payload.get("recommended_price_guidance")),
        best_next_commercial_move=_norm(payload.get("best_next_commercial_move")),
        buyer_email_subject=_norm(payload.get("buyer_email_subject")),
        buyer_email_body=_norm(payload.get("buyer_email_body")),
        internal_operator_note=_norm(payload.get("internal_operator_note")),
        reply_state=_norm(payload.get("reply_state")),
        reply_text=_norm(payload.get("reply_text")),
        payment_link=_norm(payload.get("payment_link")),
        current_close_state=_norm(payload.get("current_close_state") or "new") or "new",
    )


def _first_name(name: str) -> str:
    return name.split(" ")[0] if name else "there"


def _payment_link_for(payload: CloseOpportunity) -> str:
    if payload.payment_link:
        return payload.payment_link
    return settings.packet_checkout_url


def _proposal_email(payload: CloseOpportunity, payment_link: str) -> tuple[str, str]:
    first_name = _first_name(payload.contact_name)
    subject = f"{payload.company_name} — {payload.recommended_offer}"
    body = (
        f"Hi {first_name},\n\n"
        f"Current move: {payload.recommended_offer.lower()}. "
        f"{payload.best_next_commercial_move}\n\n"
        f"Current pricing guidance: {payload.recommended_price_guidance}.\n\n"
        f"If you want to move ahead, you can use this link to start the paid step:\n{payment_link}\n\n"
        "Once that is in, I’ll queue the work and move the process forward from there.\n\n"
        "— Alan"
    )
    return subject, body


def _clarify_email(payload: CloseOpportunity) -> tuple[str, str]:
    first_name = _first_name(payload.contact_name)
    subject = f"{payload.company_name} — one thing to clarify"
    body = (
        f"Hi {first_name},\n\n"
        "Before I push this into a paid step, I want to confirm one real example so the scope is right. "
        "Reply with one recent call or a clearer version of the current bottleneck and I’ll point you to the right next move.\n\n"
        "— Alan"
    )
    return subject, body


def _followup_wait_email(payload: CloseOpportunity) -> tuple[str, str]:
    first_name = _first_name(payload.contact_name)
    subject = f"{payload.company_name} — checking timing"
    body = (
        f"Hi {first_name},\n\n"
        "No problem. I’ll leave this here for now. "
        "If timing changes, send one concrete sales-call example and I can re-open the next step quickly.\n\n"
        "— Alan"
    )
    return subject, body


def decide_close_path(payload_dict: Dict[str, Any]) -> ClosePathDecision:
    payload = parse_close_opportunity(payload_dict)
    payment_link = _payment_link_for(payload)

    # payment event overrides everything else
    if payload.reply_state == "payment_received":
        return ClosePathDecision(
            opportunity_id=payload.opportunity_id,
            company_name=payload.company_name,
            current_close_state=payload.current_close_state,
            next_close_state="fulfillment_queued",
            close_route="trigger_fulfillment",
            subject="",
            body="",
            payment_link_used=payment_link,
            fulfillment_should_trigger=True,
            fulfillment_trigger_reason="payment received; queue fulfillment immediately",
            next_action="Queue fulfillment now.",
            founder_digest_line=f"{payload.company_name} | paid | fulfillment_queued",
            state_record={
                "opportunity_id": payload.opportunity_id,
                "company_name": payload.company_name,
                "close_state": "fulfillment_queued",
                "launch_type": payload.launch_type,
                "recommended_offer": payload.recommended_offer,
                "payment_link": payment_link,
            },
        )

    if payload.reply_state == "replied_interested":
        subject, body = _proposal_email(payload, payment_link)
        return ClosePathDecision(
            opportunity_id=payload.opportunity_id,
            company_name=payload.company_name,
            current_close_state=payload.current_close_state,
            next_close_state="proposal_sent",
            close_route="send_proposal",
            subject=subject,
            body=body,
            payment_link_used=payment_link,
            fulfillment_should_trigger=False,
            fulfillment_trigger_reason="",
            next_action="Send proposal/paid-step email and wait for payment or questions.",
            founder_digest_line=f"{payload.company_name} | interested | proposal_sent | {payload.recommended_offer}",
            state_record={
                "opportunity_id": payload.opportunity_id,
                "company_name": payload.company_name,
                "close_state": "proposal_sent",
                "launch_type": payload.launch_type,
                "recommended_offer": payload.recommended_offer,
                "payment_link": payment_link,
            },
        )

    if payload.reply_state == "replied_needs_info":
        subject, body = _clarify_email(payload)
        return ClosePathDecision(
            opportunity_id=payload.opportunity_id,
            company_name=payload.company_name,
            current_close_state=payload.current_close_state,
            next_close_state="needs_info",
            close_route="send_clarify",
            subject=subject,
            body=body,
            payment_link_used="",
            fulfillment_should_trigger=False,
            fulfillment_trigger_reason="",
            next_action="Send clarify email and wait for the missing example/context.",
            founder_digest_line=f"{payload.company_name} | needs_info | clarify sent",
            state_record={
                "opportunity_id": payload.opportunity_id,
                "company_name": payload.company_name,
                "close_state": "needs_info",
                "launch_type": payload.launch_type,
                "recommended_offer": payload.recommended_offer,
                "payment_link": "",
            },
        )

    if payload.reply_state == "replied_not_now":
        subject, body = _followup_wait_email(payload)
        return ClosePathDecision(
            opportunity_id=payload.opportunity_id,
            company_name=payload.company_name,
            current_close_state=payload.current_close_state,
            next_close_state="not_now",
            close_route="wait",
            subject=subject,
            body=body,
            payment_link_used="",
            fulfillment_should_trigger=False,
            fulfillment_trigger_reason="",
            next_action="Do not push. Set later follow-up and suppress routine attention.",
            founder_digest_line=f"{payload.company_name} | not_now | follow up later",
            state_record={
                "opportunity_id": payload.opportunity_id,
                "company_name": payload.company_name,
                "close_state": "not_now",
                "launch_type": payload.launch_type,
                "recommended_offer": payload.recommended_offer,
                "payment_link": "",
            },
        )

    if payload.reply_state == "replied_not_fit":
        return ClosePathDecision(
            opportunity_id=payload.opportunity_id,
            company_name=payload.company_name,
            current_close_state=payload.current_close_state,
            next_close_state="closed_lost",
            close_route="close_lost",
            subject="",
            body="",
            payment_link_used="",
            fulfillment_should_trigger=False,
            fulfillment_trigger_reason="",
            next_action="Mark lost and remove from active attention.",
            founder_digest_line=f"{payload.company_name} | not_fit | closed_lost",
            state_record={
                "opportunity_id": payload.opportunity_id,
                "company_name": payload.company_name,
                "close_state": "closed_lost",
                "launch_type": payload.launch_type,
                "recommended_offer": payload.recommended_offer,
                "payment_link": "",
            },
        )

    # default fallback
    return ClosePathDecision(
        opportunity_id=payload.opportunity_id,
        company_name=payload.company_name,
        current_close_state=payload.current_close_state,
        next_close_state=payload.current_close_state,
        close_route="wait",
        subject="",
        body="",
        payment_link_used="",
        fulfillment_should_trigger=False,
        fulfillment_trigger_reason="",
        next_action="No close action yet.",
        founder_digest_line=f"{payload.company_name} | no close action",
        state_record={
            "opportunity_id": payload.opportunity_id,
            "company_name": payload.company_name,
            "close_state": payload.current_close_state,
            "launch_type": payload.launch_type,
            "recommended_offer": payload.recommended_offer,
            "payment_link": "",
        },
    )


def close_path_to_dict(decision: ClosePathDecision) -> Dict[str, Any]:
    return asdict(decision)
