from __future__ import annotations

import asyncio

from fastapi import APIRouter, BackgroundTasks, Body, Depends

from app.api.admin_auth import require_relay_admin
from app.core.config import relay_costs_paused, relay_paused_response
from app.services.autonomous_ops import (
    daily_series,
    money_summary,
    monthly_summary,
    ops_status,
    run_autonomous_cycle,
    run_paid_lifecycle_tick,
    send_daily_money_summary,
)
from app.services.relay_success_controller import relay_success_status, run_relay_success_control_tick

router = APIRouter()


def _body_bool(body: dict, key: str, default: bool) -> bool:
    value = body.get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@router.get("/status")
async def autonomous_status(_: None = Depends(require_relay_admin)) -> dict:
    return ops_status()


@router.get("/money-summary")
async def autonomous_money_summary(_: None = Depends(require_relay_admin)) -> dict:
    return money_summary()


@router.get("/success")
async def autonomous_success(_: None = Depends(require_relay_admin)) -> dict:
    return relay_success_status()


@router.post("/success-tick")
def autonomous_success_tick(_: None = Depends(require_relay_admin)) -> dict:
    if relay_costs_paused():
        return {
            "status": "paid_lifecycle_only",
            "summary": "cost pause active; ran paid-customer reminders and upsell only",
            "pause": relay_paused_response("autonomous_success_tick"),
            "paid_lifecycle": run_paid_lifecycle_tick(),
        }
    return run_relay_success_control_tick()


@router.get("/daily-series")
async def autonomous_daily_series(
    days: int | None = None,
    _: None = Depends(require_relay_admin),
) -> dict:
    return daily_series(days=days)


@router.get("/monthly-summary")
async def autonomous_monthly_summary(
    days: int = 30,
    _: None = Depends(require_relay_admin),
) -> dict:
    return monthly_summary(days=days)


@router.post("/run")
def autonomous_run(
    background_tasks: BackgroundTasks,
    body: dict = Body(default={}),
    _: None = Depends(require_relay_admin),
) -> dict:
    if relay_costs_paused():
        background_tasks.add_task(_run_paid_lifecycle_sync)
        return {
            "status": "accepted",
            "summary": "cost pause active; paid lifecycle tick queued",
            "pause": relay_paused_response("autonomous_run"),
            "send_live": False,
            "notify": False,
        }
    force_query = body.get("q_keywords")
    send_live = _body_bool(body, "send_live", True)
    notify = _body_bool(body, "notify", True)

    background_tasks.add_task(_run_sync, force_query, send_live, notify)
    return {
        "status": "accepted",
        "summary": "autonomous cycle queued",
        "send_live": send_live,
        "notify": notify,
    }


@router.post("/send-daily-summary")
def trigger_daily_summary(_: None = Depends(require_relay_admin)) -> dict:
    if relay_costs_paused():
        return relay_paused_response("autonomous_send_daily_summary")
    return send_daily_money_summary()


def _run_sync(force_query: str | None, send_live: bool, notify: bool) -> None:
    if relay_costs_paused():
        run_paid_lifecycle_tick()
        return
    try:
        asyncio.run(
            run_autonomous_cycle(
                force_query=force_query,
                send_live=send_live,
                notify=notify,
            )
        )
    except Exception as exc:
        print("autonomous_run error:", exc)


def _run_paid_lifecycle_sync() -> None:
    try:
        run_paid_lifecycle_tick()
    except Exception as exc:
        print("paid_lifecycle_run error:", exc)
