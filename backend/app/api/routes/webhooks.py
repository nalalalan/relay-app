import json
import os

from fastapi import APIRouter, Header, HTTPException, Request

from app.api.routes.acquisition_supervisor import (
    supervisor_intake_webhook,
    supervisor_smartlead_webhook,
    supervisor_stripe_webhook,
)
from app.core.config import relay_costs_paused, relay_paused_response
from app.services.buyer_pilot import process_buyer_pilot_submission
from app.services.post_purchase_autopilot import (
    send_intake_ack_for_email,
    send_paid_onboarding_for_email,
)
from app.services.state_machine import EventType, StateMachineService
from app.services.stripe_webhook_security import (
    StripeSignatureError,
    verify_stripe_signature_header,
)
from app.workers.fulfillment import process_tally_submission

router = APIRouter()
state_machine = StateMachineService()


def _stripe_email(payload: dict) -> str:
    obj = payload.get("data", {}).get("object", {})
    return str(
        obj.get("customer_details", {}).get("email")
        or obj.get("customer_email")
        or ""
    ).strip().lower()


def _tally_email(payload: dict) -> str:
    data = payload.get("data") or {}
    fields = data.get("fields") or payload.get("fields") or []
    for field in fields:
        key = str(field.get("key") or field.get("label") or "").lower()
        if "email" in key:
            value = field.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
    return str(payload.get("email") or payload.get("contact_email") or "").strip().lower()


@router.post("/stripe")
async def stripe_webhook(request: Request, stripe_signature: str | None = Header(default=None)) -> dict:
    raw_body = await request.body()
    try:
        signature = verify_stripe_signature_header(
            raw_body,
            stripe_signature,
            tolerance_seconds=int(os.getenv("STRIPE_WEBHOOK_TOLERANCE_SECONDS", "300") or "300"),
        )
    except StripeSignatureError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = json.loads(raw_body.decode("utf-8") or "{}")
    acquisition_result = await supervisor_stripe_webhook(_FakeRequest(payload))

    event_type = payload.get("type", "unknown")
    if event_type == "checkout.session.completed":
        state_machine.apply_event(EventType.PAID)

    autopilot = {}
    email = _stripe_email(payload)
    if email and relay_costs_paused():
        autopilot = relay_paused_response("stripe_paid_onboarding")
    elif email:
        autopilot = send_paid_onboarding_for_email(email)

    return {
        "status": "received",
        "provider": "stripe",
        "event_type": event_type,
        "signature": signature,
        "acquisition": acquisition_result,
        "autopilot": autopilot,
    }


@router.post("/buyer-pilot")
async def buyer_pilot_webhook(request: Request, x_tally_signature: str | None = Header(default=None)) -> dict:
    payload = await request.json()
    if not payload:
        raise HTTPException(status_code=400, detail="empty payload")
    if relay_costs_paused():
        return {
            "provider": "tally",
            "flow": "buyer_pilot",
            **relay_paused_response("buyer_pilot_webhook"),
        }

    result = process_buyer_pilot_submission(payload)
    return {
        "status": "processed",
        "provider": "tally",
        "flow": "buyer_pilot",
        **result,
    }


@router.post("/tally")
async def tally_webhook(request: Request, x_tally_signature: str | None = Header(default=None)) -> dict:
    payload = await request.json()
    if not payload:
        raise HTTPException(status_code=400, detail="empty payload")

    acquisition_result = await supervisor_intake_webhook(_FakeRequest(payload))
    if relay_costs_paused():
        return {
            "provider": "tally",
            "flow": "client_fulfillment",
            "acquisition": acquisition_result,
            **relay_paused_response("tally_fulfillment_webhook"),
        }

    result = process_tally_submission(payload)

    state_machine.apply_event(EventType.SUBMITTED)
    state_machine.apply_event(EventType.GENERATED)
    if result.get("sending", {}).get("sent_count", 0) > 0:
        state_machine.apply_event(EventType.SENT)

    autopilot = {}
    email = _tally_email(payload)
    if email and acquisition_result.get("status") == "processed":
        autopilot = send_intake_ack_for_email(email)

    return {
        "status": "processed",
        "provider": "tally",
        "flow": "client_fulfillment",
        "acquisition": acquisition_result,
        "autopilot": autopilot,
        **result,
    }


@router.post("/smartlead")
async def smartlead_webhook(request: Request) -> dict:
    payload = await request.json()
    acquisition_result = await supervisor_smartlead_webhook(_FakeRequest(payload))

    event_type = payload.get("event_type") or payload.get("type", "unknown")
    smartlead_event_map = {
        "EMAIL_SENT": EventType.OUTREACHED,
        "EMAIL_REPLIED": EventType.REPLIED,
    }

    mapped = smartlead_event_map.get(event_type)
    if mapped is not None:
        state_machine.apply_event(mapped)

    return {
        "status": "received",
        "provider": "smartlead",
        "event_type": event_type,
        "acquisition": acquisition_result,
    }


@router.post("/resend")
async def resend_webhook(request: Request) -> dict[str, str]:
    payload = await request.json()
    event_type = payload.get("type", "unknown")
    if event_type in {"email.delivered", "email.sent"}:
        state_machine.apply_event(EventType.SENT)
    return {"status": "received", "provider": "resend", "event_type": event_type}


class _FakeRequest:
    def __init__(self, payload: dict):
        self._payload = payload

    async def json(self) -> dict:
        return self._payload
