from __future__ import annotations

import asyncio

from fastapi import APIRouter, BackgroundTasks, Body, Depends

from app.api.admin_auth import require_relay_admin
from app.services.autonomous_ops import (
    daily_series,
    money_summary,
    monthly_summary,
    ops_status,
    run_autonomous_cycle,
    send_daily_money_summary,
)

router = APIRouter()


@router.get("/status")
async def autonomous_status(_: None = Depends(require_relay_admin)) -> dict:
    return ops_status()


@router.get("/money-summary")
async def autonomous_money_summary(_: None = Depends(require_relay_admin)) -> dict:
    return money_summary()


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
    force_query = body.get("q_keywords")
    send_live = body.get("send_live", True)
    notify = body.get("notify", True)

    background_tasks.add_task(_run_sync, force_query, send_live, notify)
    return {"status": "accepted", "summary": "autonomous cycle queued"}


@router.post("/send-daily-summary")
def trigger_daily_summary(_: None = Depends(require_relay_admin)) -> dict:
    return send_daily_money_summary()


def _run_sync(force_query: str | None, send_live: bool, notify: bool) -> None:
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
