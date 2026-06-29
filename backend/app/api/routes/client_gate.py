from __future__ import annotations

from json import JSONDecodeError
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Request

from app.core.config import relay_paused_response
from app.services.acquisition_supervisor import handle_intake_webhook
from app.services.client_gate_v1 import redeem_client_access_code
from app.services.post_purchase_autopilot import paid_customer_can_fulfill_email, send_intake_ack_for_email
from app.services.state_machine import EventType, StateMachineService
from app.workers.fulfillment import process_tally_submission

router = APIRouter()
state_machine = StateMachineService()


def _invalid_request() -> dict:
    return {"status": "error", "message": "invalid access request", "client_form_url": "", "client_label": ""}


async def _read_json(request: Request) -> dict[str, Any] | None:
    try:
        payload = await request.json()
    except JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _text(payload: dict[str, Any], key: str) -> str:
    return str(payload.get(key, "") or "").strip()


def _paid_intake_payload(payload: dict[str, Any]) -> dict[str, Any]:
    email = _text(payload, "email").lower()
    return {
        "data": {
            "submissionId": f"client-gate-{uuid4().hex}",
            "fields": [
                {"label": "Where should we send your follow-up email?", "value": email},
                {"label": "Client or company name", "value": _text(payload, "client_name")},
                {"label": "What should this focus on?", "value": _text(payload, "focus")},
                {"label": "Preferred tone for the follow-up email", "value": _text(payload, "tone")},
                {
                    "label": "Paste your stuck client email, last reply, rough draft, or bullets",
                    "value": _text(payload, "raw_notes"),
                },
            ],
        }
    }


@router.post("/redeem")
async def redeem(request: Request) -> dict:
    payload = await _read_json(request)
    if payload is None:
        return _invalid_request()
    code = str(payload.get("access_code", "")).strip()
    result = redeem_client_access_code(code)
    return result.__dict__


@router.post("/submit")
async def submit_paid_intake(request: Request) -> dict:
    payload = await _read_json(request)
    if payload is None:
        return _invalid_request()

    access_code = _text(payload, "access_code")
    gate = redeem_client_access_code(access_code)
    if gate.status != "ok":
        return gate.__dict__

    email = _text(payload, "email").lower()
    raw_notes = _text(payload, "raw_notes")
    if not email:
        return {"status": "error", "message": "email is required"}
    if not raw_notes:
        return {"status": "error", "message": "client notes are required"}
    if not paid_customer_can_fulfill_email(email):
        pause = relay_paused_response("client_gate_submit:unpaid_email")
        return {
            "status": "denied",
            "message": "paid email not found for this intake",
            "provider": "client_gate",
            "flow": "client_fulfillment",
            "pause_reason": pause.get("reason"),
            "summary": pause.get("summary"),
        }

    fulfillment_payload = _paid_intake_payload({**payload, "email": email})
    acquisition_result = handle_intake_webhook(fulfillment_payload)
    result = process_tally_submission(fulfillment_payload)

    state_machine.apply_event(EventType.SUBMITTED)
    state_machine.apply_event(EventType.GENERATED)
    if result.get("sending", {}).get("sent_count", 0) > 0:
        state_machine.apply_event(EventType.SENT)

    autopilot = {}
    if acquisition_result.get("status") == "processed":
        autopilot = send_intake_ack_for_email(email)

    return {
        "status": "processed",
        "provider": "client_gate",
        "flow": "client_fulfillment",
        "acquisition": acquisition_result,
        "autopilot": autopilot,
        **result,
    }
