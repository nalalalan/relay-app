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
from app.services.relay_success_controller import relay_success_status

router = APIRouter()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _nested(mapping: dict, key: str) -> dict:
    value = mapping.get(key) if isinstance(mapping, dict) else {}
    return value if isinstance(value, dict) else {}


def _operator_mode(status: dict, success: dict) -> dict:
    snapshot = _nested(success, "snapshot")
    money = _nested(snapshot, "money")
    intent = _nested(snapshot, "intent")
    outreach = _nested(snapshot, "outreach")

    bottleneck = str(success.get("bottleneck") or "").strip()
    payments = _safe_int(money.get("payments"))
    replies = _safe_int(
        outreach.get("replies")
        or status.get("total_replies_all_time")
        or status.get("replies_today")
    )
    checkout_clicks = _safe_int(intent.get("checkout_clicks"))
    due = _safe_int(outreach.get("due_now") or status.get("due_now_count"))
    cap_remaining = _safe_int(outreach.get("cap_remaining") or status.get("cap_remaining"))
    active_due = _safe_int(
        outreach.get("active_experiment_new_due_count")
        or status.get("active_experiment_new_due_count")
    )
    active_needs_sample = bool(
        outreach.get("active_experiment_needs_sample")
        or status.get("active_experiment_needs_sample")
    )
    ready_due = active_due if active_needs_sample else due
    send_window_open = bool(outreach.get("send_window_is_open") or status.get("send_window_is_open"))
    money_loop = _nested(status, "money_loop")
    loop_status = str(money_loop.get("status") or "").strip()

    urgent = {
        "infrastructure_blocked",
        "paid_fulfillment",
        "checkout_to_payment",
        "reply_to_payment",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
    }
    followup = {"messy_notes_to_payment", "sample_to_notes"}

    if loop_status in {"disabled", "error", "stuck", "late"}:
        return {"mode": "attention_required", "do_not_interrupt_user": False, "reason": f"money loop is {loop_status}"}
    if bottleneck in urgent or replies > payments or checkout_clicks > payments:
        return {
            "mode": "attention_required",
            "do_not_interrupt_user": False,
            "reason": success.get("next_action") or bottleneck or "buyer signal needs handling",
        }
    if bottleneck in followup:
        return {
            "mode": "autonomous_followup_due",
            "do_not_interrupt_user": True,
            "reason": success.get("next_action") or "Relay has a follow-up queued",
        }
    if ready_due > 0 and cap_remaining > 0 and send_window_open:
        return {
            "mode": "autonomous_sending_now",
            "do_not_interrupt_user": True,
            "reason": "send window is open and queued leads are ready",
        }
    if ready_due > 0 and cap_remaining > 0:
        return {
            "mode": "out_of_loop_waiting",
            "do_not_interrupt_user": True,
            "reason": "queued leads are ready for the next send window",
        }
    return {
        "mode": "out_of_loop_monitoring",
        "do_not_interrupt_user": True,
        "reason": success.get("next_action") or "no immediate human action",
    }


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator <= 0:
        return 0
    return (numerator + max(denominator, 1) - 1) // max(denominator, 1)


def _autonomous_plan(status: dict, operator: dict, success: dict) -> dict:
    active_sends = _safe_int(status.get("active_experiment_sends"))
    active_target = _safe_int(status.get("active_experiment_sample_target"))
    active_due = _safe_int(status.get("active_experiment_new_due_count"))
    active_remaining = max(active_target - active_sends, 0) if active_target else 0
    cap_remaining = _safe_int(status.get("cap_remaining"))
    daily_cap = _safe_int(status.get("daily_send_cap"))
    capacity = max(min(cap_remaining or daily_cap, daily_cap or cap_remaining or 1), 1)
    windows_remaining = _ceil_div(active_remaining, capacity)
    next_window = str(status.get("send_window_next_open_local") or "").strip()
    mode = str(operator.get("mode") or "").strip()

    if mode == "attention_required":
        phase = "human_signal"
        summary = str(operator.get("reason") or "Handle the real signal before changing volume.")
    elif active_target and active_remaining > 0:
        phase = "collect_active_sample"
        summary = (
            f"Collect {active_remaining} more active-variant sends before judging. "
            f"At the current cap, that is about {windows_remaining} send window"
            f"{'' if windows_remaining == 1 else 's'}."
        )
    else:
        phase = "judge_or_rotate"
        summary = str(success.get("next_action") or "Review reply and payment signal before changing volume.")

    return {
        "phase": phase,
        "summary": summary,
        "active_experiment_progress": f"{active_sends}/{active_target}" if active_target else "",
        "active_experiment_remaining": active_remaining,
        "active_experiment_due_now": active_due,
        "estimated_windows_remaining": windows_remaining,
        "next_autonomous_window": next_window,
        "review_rule": "do not judge the offer until the active sample is complete or real buyer signal appears",
    }


def _status_with_operator_mode() -> dict:
    status = outreach_status()
    try:
        success = relay_success_status()
    except Exception as error:
        success = {"status": "error", "bottleneck": "status_unavailable", "next_action": str(error)}
    operator = _operator_mode(status, success)
    status["operator_mode"] = operator
    status["autonomous_plan"] = _autonomous_plan(status, operator, success)
    status["relay_success_bottleneck"] = success.get("bottleneck")
    status["relay_success_next_action"] = success.get("next_action")
    return status


@router.get("/status")
async def custom_outreach_status(_: None = Depends(require_relay_admin)) -> dict:
    return _status_with_operator_mode()


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
