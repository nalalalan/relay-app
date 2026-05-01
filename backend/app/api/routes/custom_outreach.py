from __future__ import annotations

from fastapi import APIRouter, Body, Depends

from app.api.admin_auth import require_relay_admin
from app.services.custom_outreach import (
    outreach_status,
    poll_reply_mailbox,
    run_custom_outreach_cycle,
    send_due_sequence_messages,
    send_test_email,
)
from app.services.relay_performance import (
    relay_performance_status,
    run_weekly_performance_review,
)

router = APIRouter()


@router.get("/status")
async def custom_outreach_status(_: None = Depends(require_relay_admin)) -> dict:
    return outreach_status()


@router.post("/run")
async def custom_outreach_run(_: None = Depends(require_relay_admin)) -> dict:
    return run_custom_outreach_cycle()


@router.post("/send-due")
async def custom_outreach_send_due(_: None = Depends(require_relay_admin)) -> dict:
    return send_due_sequence_messages()


@router.post("/check-replies")
async def custom_outreach_check_replies(_: None = Depends(require_relay_admin)) -> dict:
    return poll_reply_mailbox()


@router.post("/send-test")
async def custom_outreach_send_test(
    body: dict = Body(default={}),
    _: None = Depends(require_relay_admin),
) -> dict:
    to_email = body.get("to_email", "")
    return send_test_email(to_email)


@router.get("/performance")
async def relay_performance(_: None = Depends(require_relay_admin)) -> dict:
    return relay_performance_status()


@router.post("/performance/run-weekly-review")
async def relay_performance_weekly_review(
    body: dict = Body(default={}),
    _: None = Depends(require_relay_admin),
) -> dict:
    return run_weekly_performance_review(
        force=bool(body.get("force", False)),
        fetch_research=bool(body.get("fetch_research", True)),
    )
