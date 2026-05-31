from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request

from app.api.admin_auth import require_relay_admin
from app.core.config import relay_costs_paused, relay_paused_response
from app.services.acquisition_supervisor import (
    acquisition_digest,
    handle_intake_webhook,
    handle_smartlead_reply_webhook,
    handle_stripe_purchase_webhook,
    import_from_apollo_people_search,
    import_from_apollo_search,
    tick_supervisor,
)
from app.services.stripe_webhook_security import (
    StripeSignatureError,
    verify_stripe_signature_header,
)

router = APIRouter()


@router.get("/digest")
async def supervisor_digest(_: None = Depends(require_relay_admin)) -> dict:
    return acquisition_digest()


@router.post("/apollo-search")
def apollo_search(
    body: dict,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_relay_admin),
) -> dict:
    if relay_costs_paused():
        return relay_paused_response("acquisition_apollo_search")
    background_tasks.add_task(run_apollo_search, body)
    return {"status": "accepted"}


@router.post("/apollo-people-search")
def apollo_people_search(
    body: dict,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_relay_admin),
) -> dict:
    if relay_costs_paused():
        return relay_paused_response("acquisition_apollo_people_search")
    background_tasks.add_task(run_apollo_people_search, body)
    return {"status": "accepted"}


@router.post("/tick")
def tick(
    body: dict,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_relay_admin),
) -> dict:
    if relay_costs_paused():
        return relay_paused_response("acquisition_tick")
    background_tasks.add_task(run_tick, body)
    return {"status": "accepted"}


@router.post("/webhooks/smartlead")
async def supervisor_smartlead_webhook(request: Request) -> dict:
    payload = await request.json()
    return await handle_smartlead_reply_webhook(payload)


@router.post("/webhooks/stripe")
async def supervisor_stripe_webhook(request: Request, stripe_signature: str | None = Header(default=None)) -> dict:
    raw_body = await request.body() if hasattr(request, "body") else b""
    if raw_body:
        try:
            verify_stripe_signature_header(
                raw_body,
                stripe_signature,
                tolerance_seconds=int(os.getenv("STRIPE_WEBHOOK_TOLERANCE_SECONDS", "300") or "300"),
            )
        except StripeSignatureError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = await request.json()
    return handle_stripe_purchase_webhook(payload)


@router.post("/webhooks/intake")
async def supervisor_intake_webhook(request: Request) -> dict:
    payload = await request.json()
    return handle_intake_webhook(payload)


def run_apollo_search(body: dict) -> None:
    if relay_costs_paused():
        return
    try:
        asyncio.run(import_from_apollo_search(body))
    except Exception as e:
        print("apollo_search error:", e)


def run_apollo_people_search(body: dict) -> None:
    if relay_costs_paused():
        return
    try:
        asyncio.run(import_from_apollo_people_search(body))
    except Exception as e:
        print("apollo_people_search error:", e)


def run_tick(body: dict) -> None:
    if relay_costs_paused():
        return
    try:
        send_live = body.get("send_live", True)
        asyncio.run(tick_supervisor(send_live=send_live))
    except Exception as e:
        print("tick error:", e)
