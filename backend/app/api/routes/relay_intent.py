import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func

from app.core.config import settings
from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.models.production_wiring import (
    ProductionAction,
    ProductionException,
    ProductionLead,
    ProductionOpportunity,
    ProductionTransition,
)
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead
from app.services.relay_research_journal import RELAY_RESEARCH_JOURNAL_EVENT


router = APIRouter()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _relay_url(path: str = "") -> str:
    base = (settings.landing_page_url or "https://relaybrief.com").rstrip("/")
    clean_path = path if path.startswith("/") else f"/{path}" if path else ""
    return f"{base}{clean_path}"


def _sample_packet_url() -> str:
    return (
        os.getenv("RELAY_SAMPLE_URL", "").strip()
        or os.getenv("SAMPLE_PDF_URL", "").strip()
        or "https://raw.githubusercontent.com/nalalalan/relay-live/main/sample.pdf"
    )


class RelayIntentEventIn(BaseModel):
    session_id: str | None = None
    event_type: str = Field(..., min_length=1, max_length=80)
    path: str | None = None
    page_url: str | None = None
    target_text: str | None = None
    target_href: str | None = None
    referrer: str | None = None
    metadata: dict[str, Any] | None = None


class RelayIntentLeadIn(BaseModel):
    session_id: str | None = None
    email: str = Field(..., min_length=3, max_length=320)
    source: str | None = "sample_request"
    page_url: str | None = None
    referrer: str | None = None
    metadata: dict[str, Any] | None = None


def _session_id(value: str | None) -> str:
    clean = (value or "").strip()
    return clean[:128] if clean else str(uuid.uuid4())


def _json(data: dict[str, Any] | None) -> str | None:
    if not data:
        return None
    return json.dumps(data, ensure_ascii=False, sort_keys=True)[:8000]


def _safe_payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator <= 0 or denominator <= 0:
        return 0
    return (numerator + denominator - 1) // denominator


def _configured_offer_url(env_name: str, setting_name: str) -> str:
    return (
        os.getenv(env_name, "").strip()
        or str(getattr(settings, setting_name, "") or "").strip()
    )


def _revenue_ladder_status() -> dict[str, Any]:
    entry_url = str(settings.packet_checkout_url or "").strip()
    offers = [
        ("entry_packet", "one live packet", bool(entry_url)),
        ("five_pack", "5-call sprint", bool(_configured_offer_url("PACKET_5_PACK_URL", "packet_5_pack_url"))),
        ("weekly_sprint", "done-for-you week", bool(_configured_offer_url("WEEKLY_SPRINT_URL", "weekly_sprint_url"))),
        (
            "monthly_autopilot",
            "done-for-you month",
            bool(_configured_offer_url("MONTHLY_AUTOPILOT_URL", "monthly_autopilot_url")),
        ),
    ]
    configured = [
        {"key": key, "label": label}
        for key, label, available in offers
        if available
    ]
    higher_offer_count = max(len(configured) - (1 if entry_url else 0), 0)
    return {
        "mode": "ladder" if higher_offer_count > 0 else "entry_offer_only",
        "configured_offer_count": len(configured),
        "higher_offer_count": higher_offer_count,
        "configured_offers": configured,
        "entry_offer_ready": bool(entry_url),
        "higher_offers_ready": higher_offer_count > 0,
    }


def _money_objective_status(*, money: dict[str, Any], outreach: dict[str, Any], payments: int) -> dict[str, Any]:
    target = outreach.get("money_target") if isinstance(outreach.get("money_target"), dict) else {}
    weekly_target_usd = _safe_float(
        target.get("weekly_target_usd") or os.getenv("RELAY_WEEKLY_TARGET_USD", "100"),
        100.0,
    )
    test_price_usd = max(
        _safe_float(target.get("test_price_usd") or os.getenv("RELAY_PACKET_PRICE_USD", "40"), 40.0),
        1.0,
    )
    paid_tests_needed = max(
        1,
        _safe_int(target.get("paid_tests_needed_weekly"), math.ceil(weekly_target_usd / test_price_usd)),
    )
    current_gross_usd = round(_safe_float(money.get("gross_usd"), 0.0), 2)
    revenue_gap_usd = round(max(weekly_target_usd - current_gross_usd, 0.0), 2)
    paid_tests_remaining = max(paid_tests_needed - payments, 0)
    daily_cap = _safe_int(
        target.get("current_daily_send_cap")
        or outreach.get("effective_daily_cap")
        or outreach.get("daily_send_cap")
    )
    business_week_send_capacity = _safe_int(target.get("business_week_send_capacity"), daily_cap * 5)

    return {
        "state": "weekly_target_met" if revenue_gap_usd <= 0 else "needs_paid_tests",
        "weekly_target_usd": weekly_target_usd,
        "entry_test_price_usd": test_price_usd,
        "current_week_gross_usd": current_gross_usd,
        "revenue_gap_usd": revenue_gap_usd,
        "paid_tests_needed_weekly": paid_tests_needed,
        "paid_tests_remaining_weekly": paid_tests_remaining,
        "current_week_payments": payments,
        "current_daily_send_cap": daily_cap,
        "business_week_send_capacity": business_week_send_capacity,
        "success_condition": (
            "weekly revenue target met"
            if revenue_gap_usd <= 0
            else f"collect {paid_tests_remaining} more paid tests or ${revenue_gap_usd:.2f} this week"
        ),
        "operating_mode": target.get("operating_mode") or "direct decision-maker inboxes first",
    }


def _money_decision_contract(
    *,
    active_sends: int,
    active_target: int,
    active_remaining: int,
    active_signal_replies: int,
    active_signal_payments: int,
    auto_closed_replies: int,
    unhandled_replies: int,
    checkout_clicks: int,
    payments: int,
    experiment_decision_next: str,
) -> dict[str, Any]:
    if active_signal_payments > 0:
        state = "paid_signal_keep_stable"
        next_action = "Keep the current lane stable and fulfill the paid buyer."
    elif auto_closed_replies > 0 and unhandled_replies <= 0 and checkout_clicks <= payments:
        state = "auto_closed_waiting_payment"
        next_action = "Stay out; Relay auto-answered buyer interest and is waiting for checkout or payment."
    elif unhandled_replies > 0 or checkout_clicks > payments or active_signal_replies > 0:
        state = "buyer_signal_close"
        next_action = "Close real buyer signal through the paid test before changing the experiment."
    elif active_remaining > 0:
        state = "collect_sample"
        next_action = experiment_decision_next
    else:
        state = "rotate_one_variable"
        next_action = "Rotate one controlled targeting or copy variable before sending the next sample."

    return {
        "state": state,
        "next_action": next_action,
        "sample_progress": f"{active_sends}/{active_target}" if active_target else "",
        "keep_condition": "payment from the active lane",
        "close_condition": "unhandled reply or checkout intent before payment",
        "rotate_condition": "completed active sample with zero real replies and zero payments",
        "do_not_scale_condition": "do not increase volume until the active sample has buyer signal or a completed review",
    }


def _reply_autonomy_contract(
    *,
    replies: int,
    auto_replies: int,
    unhandled_replies: int,
    checkout_clicks: int,
    payments: int,
    reply_autoclose_mode: str,
) -> dict[str, Any]:
    auto_closed_replies = min(auto_replies, replies)
    if payments > 0:
        state = "paid"
        next_action = "fulfill paid buyer and keep the current lane stable"
    elif unhandled_replies > 0 or checkout_clicks > payments:
        state = "attention_required"
        next_action = "close unhandled buyer signal through the paid next step"
    elif auto_closed_replies > 0:
        state = "auto_closed_waiting_payment"
        next_action = "stay out; auto-reply already sent the paid path"
    elif replies > 0:
        state = "reply_seen_no_action"
        next_action = "watch for auto-reply or manual-review classification"
    else:
        state = "idle"
        next_action = "wait for a buyer reply"

    return {
        "state": state,
        "reply_autoclose_mode": reply_autoclose_mode or "unknown",
        "total_replies": replies,
        "auto_closed_replies": auto_closed_replies,
        "unhandled_replies": unhandled_replies,
        "checkout_clicks": checkout_clicks,
        "payments": payments,
        "next_action": next_action,
        "success_condition": "reply is auto-answered with checkout path or converted to payment",
        "interrupt_condition": "interrupt only for unhandled replies, checkout intent ahead of payment, payment fulfillment, or reply-system health failure",
    }


def _conversion_ladder_contract(
    *,
    sends: int,
    active_sends: int,
    active_target: int,
    active_remaining: int,
    replies: int,
    auto_replies: int,
    unhandled_replies: int,
    checkout_clicks: int,
    payments: int,
    revenue_objective: dict[str, Any],
    launch_readiness: dict[str, Any],
) -> dict[str, Any]:
    expected_next_sends = _safe_int(launch_readiness.get("expected_next_window_sends"))
    if payments > 0:
        state = "paid"
        active_stage = "payment"
        next_step = "fulfill paid buyer, keep the lane stable, and continue only controlled tests"
    elif checkout_clicks > payments:
        state = "checkout_open"
        active_stage = "checkout"
        next_step = "keep the paid test obvious and follow up checkout intent"
    elif unhandled_replies > 0:
        state = "reply_open"
        active_stage = "reply"
        next_step = "close unhandled reply through the paid test"
    elif auto_replies > 0:
        state = "auto_reply_sent"
        active_stage = "auto_reply"
        next_step = "wait for checkout or payment after the auto-reply"
    elif active_remaining > 0:
        state = "sample_collection"
        active_stage = "send"
        next_step = launch_readiness.get("next_window_success_criterion") or "collect the next active send batch"
    else:
        state = "sample_review"
        active_stage = "review"
        next_step = "review the completed active sample and keep or rotate one variable"

    reply_rate = round(replies / sends, 4) if sends > 0 else 0
    payment_rate = round(payments / sends, 4) if sends > 0 else 0
    paid_tests_left = _safe_int(revenue_objective.get("paid_tests_remaining_weekly"))

    return {
        "state": state,
        "active_stage": active_stage,
        "next_step": next_step,
        "stages": [
            {
                "key": "send",
                "label": "Send active sample",
                "current": active_sends,
                "target": active_target,
                "next_expected": expected_next_sends,
                "done": active_target > 0 and active_sends >= active_target,
            },
            {
                "key": "reply",
                "label": "Turn sends into replies",
                "current": replies,
                "target": 1,
                "open": unhandled_replies,
                "auto_closed": min(auto_replies, replies),
                "done": replies > 0,
            },
            {
                "key": "checkout",
                "label": "Turn replies into checkout intent",
                "current": checkout_clicks,
                "target": 1,
                "open": max(checkout_clicks - payments, 0),
                "done": checkout_clicks > 0,
            },
            {
                "key": "payment",
                "label": "Turn checkout into paid tests",
                "current": payments,
                "target": max(payments + paid_tests_left, payments, 1),
                "remaining": paid_tests_left,
                "done": paid_tests_left <= 0,
            },
        ],
        "rates": {
            "reply_rate": reply_rate,
            "payment_rate": payment_rate,
        },
        "guardrail": "do not judge conversion before the active send sample lands unless real buyer signal appears",
    }


def _success_governor_contract(
    *,
    revenue_objective: dict[str, Any],
    money_decision: dict[str, Any],
    reply_autonomy: dict[str, Any],
    launch_readiness: dict[str, Any],
    operator_mode: dict[str, Any],
) -> dict[str, Any]:
    window_contract = (
        launch_readiness.get("window_execution_contract")
        if isinstance(launch_readiness.get("window_execution_contract"), dict)
        else {}
    )
    window_state = str(window_contract.get("state") or launch_readiness.get("window_execution_state") or "").strip()
    reply_state = str(reply_autonomy.get("state") or "").strip()
    money_state = str(money_decision.get("state") or "").strip()
    revenue_state = str(revenue_objective.get("state") or "").strip()
    ready = launch_readiness.get("ready") is not False
    operator_interrupt = operator_mode.get("do_not_interrupt_user") is False

    if operator_interrupt or reply_state == "attention_required":
        state = "interrupt_required"
        next_step = reply_autonomy.get("next_action") or money_decision.get("next_action") or operator_mode.get("reason") or "handle buyer signal"
        human_policy = "interrupt Alan now"
    elif revenue_state == "weekly_target_met":
        state = "target_met_stabilize"
        next_step = "keep the current winning lane stable and fulfill paid buyers"
        human_policy = "stay out unless fulfillment or system health needs attention"
    elif reply_state == "auto_closed_waiting_payment":
        state = "wait_for_payment"
        next_step = "wait for checkout or payment after the auto-reply"
        human_policy = "stay out unless payment, unhandled reply, or health failure appears"
    elif window_state in {"window_missed", "window_underfilled"}:
        state = "fix_execution"
        next_step = window_contract.get("failure_condition") or launch_readiness.get("window_execution_failure_condition") or "fix send-window execution"
        human_policy = "interrupt because the autonomous send proof failed"
    elif not ready or window_state == "blocked":
        state = "remove_blocker"
        blockers = launch_readiness.get("blockers") if isinstance(launch_readiness.get("blockers"), list) else []
        next_step = "; ".join(str(item) for item in blockers) or window_contract.get("failure_condition") or "remove launch blocker"
        human_policy = "interrupt only if the blocker cannot be cleared autonomously"
    elif money_state == "buyer_signal_close":
        state = "close_buyer_signal"
        next_step = money_decision.get("next_action") or "close buyer signal through the paid test"
        human_policy = "stay out unless buyer signal cannot be handled automatically"
    elif money_state == "rotate_one_variable":
        state = "rotate_experiment"
        next_step = money_decision.get("next_action") or "rotate one controlled variable"
        human_policy = "stay out unless rotation fails or a buyer signal appears"
    elif money_state == "collect_sample" and window_state in {"waiting_for_capacity", "waiting_for_window", "window_open"}:
        state = "run_next_send_window"
        next_step = window_contract.get("success_criterion") or launch_readiness.get("next_window_success_criterion") or money_decision.get("next_action")
        human_policy = "stay out until the audit time unless replies, payment, or health changes"
    else:
        state = "monitor"
        next_step = money_decision.get("next_action") or launch_readiness.get("next_decision") or "keep monitoring"
        human_policy = "stay out unless replies, payment, execution miss, or system health changes"

    return {
        "state": state,
        "next_step": next_step,
        "human_policy": human_policy,
        "do_not_change": "do not change copy, target, price, or volume outside this governor",
        "success_definition": revenue_objective.get("success_condition") or "collect paid tests and protect execution quality",
        "audit_at": window_contract.get("audit_at") or launch_readiness.get("next_window_audit_at"),
        "window_state": window_state,
        "reply_state": reply_state,
        "money_decision_state": money_state,
        "revenue_state": revenue_state,
    }


def _owner_absence_contract(
    *,
    success_governor: dict[str, Any],
    launch_readiness: dict[str, Any],
    reply_autonomy: dict[str, Any],
    conversion_ladder: dict[str, Any],
    revenue_objective: dict[str, Any],
) -> dict[str, Any]:
    governor_state = str(success_governor.get("state") or "").strip()
    reply_state = str(reply_autonomy.get("state") or "").strip()
    ladder_stage = str(conversion_ladder.get("active_stage") or conversion_ladder.get("state") or "").strip()
    audit_at = success_governor.get("audit_at") or launch_readiness.get("next_window_audit_at") or ""
    blockers = launch_readiness.get("blockers") if isinstance(launch_readiness.get("blockers"), list) else []
    manual_input_required = (
        reply_state == "attention_required"
        or governor_state in {"interrupt_required", "fix_execution"}
        or (governor_state == "remove_blocker" and bool(blockers))
    )

    if manual_input_required:
        state = "owner_needed"
        reason = success_governor.get("next_step") or "; ".join(str(item) for item in blockers) or "operator interrupt required"
        autonomous_until = ""
        next_owner_interrupt = reason
    elif governor_state == "run_next_send_window":
        state = "owner_out_of_loop"
        reason = "autonomous send proof is pending"
        autonomous_until = audit_at
        next_owner_interrupt = "interrupt only for buyer signal, payment, health failure, or failed send-window proof"
    elif governor_state == "wait_for_payment":
        state = "owner_out_of_loop"
        reason = "reply was handled and Relay is waiting for checkout or payment"
        autonomous_until = "checkout, payment, unhandled reply, or reply-system health failure"
        next_owner_interrupt = "interrupt only if a human reply cannot be auto-closed or payment needs fulfillment"
    elif governor_state == "rotate_experiment":
        state = "owner_out_of_loop"
        reason = "completed sample needs one controlled autonomous rotation"
        autonomous_until = "next experiment plan is created or rotation fails"
        next_owner_interrupt = "interrupt only if rotation cannot be created or buyer signal appears"
    elif governor_state == "target_met_stabilize":
        state = "owner_out_of_loop"
        reason = "weekly target is met"
        autonomous_until = "fulfillment, system health failure, or next weekly target"
        next_owner_interrupt = "interrupt only for fulfillment or system health"
    else:
        state = "owner_out_of_loop"
        reason = success_governor.get("human_policy") or "Relay is monitoring the money loop"
        autonomous_until = audit_at or "next buyer signal, payment, execution miss, or health change"
        next_owner_interrupt = "interrupt only for buyer signal, payment, execution miss, or system health"

    if ladder_stage == "send":
        permitted_action = "send queued active leads inside the approved cap and window"
    elif ladder_stage in {"reply", "auto_reply"}:
        permitted_action = "auto-close buyer replies toward checkout when safe"
    elif ladder_stage == "checkout":
        permitted_action = "follow up checkout intent until payment or no-signal timeout"
    elif ladder_stage == "payment":
        permitted_action = "fulfill paid buyer and keep the winning lane stable"
    else:
        permitted_action = "review evidence and rotate one controlled variable only when the sample is complete"

    return {
        "state": state,
        "manual_input_required": manual_input_required,
        "reason": reason,
        "autonomous_until": autonomous_until,
        "next_owner_interrupt": next_owner_interrupt,
        "current_stage": ladder_stage,
        "permitted_action": permitted_action,
        "blocked_actions": [
            "do not ask Alan to choose copy, target, price, or volume before the proof deadline",
            "do not increase volume until buyer signal or completed review justifies it",
            "do not judge demand from an execution miss",
        ],
        "decision_rules": {
            "buyer_signal": "close to the paid test and keep the current lane stable",
            "payment": "fulfill the buyer and keep the lane stable",
            "send_window_success": "continue collecting the active sample without changing variables",
            "send_window_failure": "fix execution before judging demand",
            "sample_complete_no_signal": "rotate exactly one controlled copy or targeting variable",
        },
        "weekly_success_condition": revenue_objective.get("success_condition") or "collect paid tests",
    }


def _autonomous_money_mandate(
    *,
    revenue_objective: dict[str, Any],
    money_decision: dict[str, Any],
    reply_autonomy: dict[str, Any],
    conversion_ladder: dict[str, Any],
    success_governor: dict[str, Any],
    owner_absence: dict[str, Any],
    launch_readiness: dict[str, Any],
) -> dict[str, Any]:
    governor_state = str(success_governor.get("state") or "").strip()
    reply_state = str(reply_autonomy.get("state") or "").strip()
    money_state = str(money_decision.get("state") or "").strip()
    revenue_state = str(revenue_objective.get("state") or "").strip()
    ladder_stage = str(conversion_ladder.get("active_stage") or conversion_ladder.get("state") or "").strip()
    payments = _safe_int(revenue_objective.get("current_week_payments"))
    gross_usd = round(_safe_float(revenue_objective.get("current_week_gross_usd")), 2)
    revenue_gap_usd = round(_safe_float(revenue_objective.get("revenue_gap_usd")), 2)
    weekly_target_usd = round(_safe_float(revenue_objective.get("weekly_target_usd")), 2)
    active_remaining = _safe_int(launch_readiness.get("active_experiment_sends_remaining"))
    expected_next_sends = _safe_int(launch_readiness.get("expected_next_window_sends"))

    if revenue_state == "weekly_target_met":
        state = "protect_winning_lane"
        primary_action = success_governor.get("next_step") or "fulfill paid buyers and keep the current lane stable"
    elif governor_state in {"interrupt_required", "fix_execution", "remove_blocker"}:
        state = "restore_revenue_loop"
        primary_action = success_governor.get("next_step") or "clear the blocker before judging demand"
    elif reply_state == "attention_required" or ladder_stage in {"reply", "checkout"}:
        state = "close_buyer_signal"
        primary_action = reply_autonomy.get("next_action") or money_decision.get("next_action") or "turn buyer signal into the paid test"
    elif money_state == "rotate_one_variable" or ladder_stage == "review":
        state = "rotate_after_no_signal"
        primary_action = money_decision.get("next_action") or "rotate one controlled variable after the completed no-signal sample"
    elif active_remaining > 0 and expected_next_sends > 0:
        state = "prove_active_sample"
        primary_action = (
            launch_readiness.get("next_window_success_criterion")
            or money_decision.get("next_action")
            or "run the next send window"
        )
    elif active_remaining > 0:
        state = "create_sample_capacity"
        primary_action = launch_readiness.get("readiness_reason") or money_decision.get("next_action") or "prepare the active sample"
    else:
        state = "monitor_money_loop"
        primary_action = success_governor.get("next_step") or money_decision.get("next_action") or "keep monitoring the money loop"

    if payments > 0 or gross_usd > 0:
        money_truth = "monetized"
    elif revenue_gap_usd > 0:
        money_truth = "not_monetized_yet"
    else:
        money_truth = "target_unknown"

    manual_required = owner_absence.get("manual_input_required") is True

    return {
        "state": state,
        "money_truth": money_truth,
        "primary_metric": "paid tests",
        "score": {
            "gross_usd": gross_usd,
            "payments": payments,
            "weekly_target_usd": weekly_target_usd,
            "revenue_gap_usd": revenue_gap_usd,
        },
        "primary_action": primary_action,
        "success_condition": revenue_objective.get("success_condition") or "collect paid tests",
        "proof_deadline": success_governor.get("audit_at") or launch_readiness.get("next_window_audit_at") or "",
        "owner_policy": "manual_input_required" if manual_required else "owner_out_of_loop",
        "owner_state": owner_absence.get("state") or "",
        "current_stage": ladder_stage,
        "governor_state": governor_state,
        "allowed_autonomous_action": owner_absence.get("permitted_action") or primary_action,
        "forbidden_until_proof": [
            "do not ask Alan to choose the next move while owner_policy is owner_out_of_loop",
            "do not increase volume before buyer signal or completed sample review",
            "do not change more than one targeting, copy, price, or volume variable at a time",
            "do not declare failure from an execution miss",
        ],
        "next_human_interrupt": owner_absence.get("next_owner_interrupt") or success_governor.get("human_policy"),
    }


def _parse_contract_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _progress_current(value: Any) -> int:
    text = str(value or "").strip()
    if "/" not in text:
        return 0
    try:
        return _safe_int(text.split("/", 1)[0])
    except Exception:
        return 0


def _money_proof_health(
    *,
    money_proof_mandate: dict[str, Any],
    autonomous_money_mandate: dict[str, Any],
    launch_readiness: dict[str, Any],
    active_sends: int,
    active_remaining: int,
    payments: int,
    replies: int,
    checkout_clicks: int,
) -> dict[str, Any]:
    mandate = money_proof_mandate if money_proof_mandate else autonomous_money_mandate
    proof_state = str(mandate.get("state") or "").strip()
    deadline = (
        mandate.get("proof_deadline")
        or autonomous_money_mandate.get("proof_deadline")
        or launch_readiness.get("next_window_audit_at")
        or ""
    )
    deadline_utc = _to_utc(_parse_contract_datetime(deadline))
    now_utc = datetime.now(timezone.utc)
    overdue = deadline_utc is not None and now_utc > deadline_utc
    seconds_until_deadline = (
        int((deadline_utc - now_utc).total_seconds())
        if deadline_utc is not None
        else None
    )
    expected_next_sends = _safe_int(launch_readiness.get("expected_next_window_sends"))
    expected_progress = str(launch_readiness.get("expected_progress_after_next_window") or "").strip()
    expected_active_sends = _progress_current(expected_progress)
    if expected_active_sends <= 0 and expected_next_sends > 0:
        expected_active_sends = active_sends + expected_next_sends
    window_state = str(launch_readiness.get("window_execution_state") or "").strip()

    if payments > 0:
        state = "money_proof_satisfied"
        reason = "payment exists"
        autonomous_recovery_action = "fulfill the paid buyer and keep the current lane stable"
    elif checkout_clicks > payments or replies > 0:
        state = "buyer_signal_open"
        reason = "buyer signal is ahead of payment"
        autonomous_recovery_action = "close buyer signal through the paid test before changing the experiment"
    elif proof_state == "prove_active_sample" and expected_active_sends > 0 and active_sends >= expected_active_sends:
        state = "execution_proof_satisfied"
        reason = f"active sends reached {active_sends}, meeting the expected proof of {expected_active_sends}"
        autonomous_recovery_action = "continue the active sample without changing variables"
    elif proof_state == "prove_active_sample" and overdue:
        state = "execution_proof_missed"
        reason = f"proof deadline passed before active sends reached {expected_active_sends}"
        autonomous_recovery_action = "treat this as an execution miss, run recovery, and do not judge demand"
    elif window_state in {"window_missed", "window_underfilled"}:
        state = "execution_proof_at_risk"
        reason = f"send window state is {window_state}"
        autonomous_recovery_action = "fix send execution before judging targeting, copy, or price"
    elif proof_state in {"restore_revenue_loop", "restore_send_execution"}:
        state = "recovery_required"
        reason = mandate.get("primary_action") or "revenue loop needs recovery"
        autonomous_recovery_action = mandate.get("allowed_autonomous_action") or mandate.get("primary_action") or "restore the revenue loop"
    elif deadline_utc is not None:
        state = "waiting_for_proof_deadline"
        reason = "proof deadline has not arrived"
        autonomous_recovery_action = "stay out and let the approved autonomous action run"
    else:
        state = "watching_money_proof"
        reason = mandate.get("primary_action") or "watch the current money proof"
        autonomous_recovery_action = mandate.get("allowed_autonomous_action") or mandate.get("primary_action") or "continue the current money proof"

    return {
        "state": state,
        "reason": reason,
        "proof_state": proof_state,
        "proof_deadline": deadline,
        "seconds_until_deadline": seconds_until_deadline,
        "expected_active_sends": expected_active_sends,
        "actual_active_sends": active_sends,
        "active_remaining": active_remaining,
        "window_state": window_state,
        "autonomous_recovery_action": autonomous_recovery_action,
        "owner_interrupt": state in {"execution_proof_missed", "recovery_required"} or "buyer_signal" in state,
        "do_not_judge_demand": state in {"execution_proof_missed", "execution_proof_at_risk", "recovery_required"},
    }


def _next_window_audit_at(
    *,
    next_window: str | None,
    send_window_start: str | None,
    send_window_end: str | None,
    send_window_reason: str | None,
) -> str:
    reason = str(send_window_reason or "").strip()
    if reason == "open" and str(send_window_end or "").strip():
        return str(send_window_end or "").strip()

    next_dt = _parse_contract_datetime(next_window)
    start_dt = _parse_contract_datetime(send_window_start)
    end_dt = _parse_contract_datetime(send_window_end)
    if next_dt is not None and start_dt is not None and end_dt is not None:
        duration = end_dt - start_dt
        if duration.total_seconds() > 0:
            return (next_dt + duration).isoformat()
    return str(send_window_end or next_window or "").strip()


def _window_execution_state(
    *,
    send_window_reason: str | None,
    sent_today: int,
    cap_remaining: int,
    active_due: int,
    active_remaining: int,
    expected_next_window_sends: int,
) -> str:
    reason = str(send_window_reason or "").strip()
    if active_remaining <= 0:
        return "sample_complete"
    if expected_next_window_sends <= 0:
        if cap_remaining <= 0 and active_due > 0:
            return "waiting_for_capacity"
        return "blocked"
    if reason == "open":
        return "window_open"
    if reason == "after_window" and active_due > 0 and cap_remaining > 0 and sent_today <= 0:
        return "window_missed"
    if reason == "after_window" and active_due > 0 and cap_remaining > 0:
        return "window_underfilled"
    if reason == "after_window":
        return "window_passed"
    return "waiting_for_window"


def _launch_readiness_state(
    *,
    blockers: list[str],
    active_remaining: int,
    active_due: int,
    cap_remaining: int,
    expected_next_window_sends: int,
    window_execution_state: str,
) -> tuple[str, str]:
    if blockers:
        return "blocked", "; ".join(blockers)
    if active_remaining <= 0:
        return "sample_ready_for_review", "active sample is complete and ready for the keep-or-rotate decision"
    if expected_next_window_sends > 0 and window_execution_state == "window_open":
        return "ready_sending_now", "send window is open with queued active leads and capacity"
    if expected_next_window_sends > 0:
        return "ready_waiting_for_window", "queued active leads and capacity are ready for the next send window"
    if active_due <= 0:
        return "needs_active_leads", "active experiment needs more queued first-touch leads"
    if cap_remaining <= 0:
        return "waiting_for_capacity", "daily send capacity is exhausted"
    return "needs_send_window_capacity", "send window capacity is not available yet"


def _operator_mode(
    *,
    state: str,
    next_action: str = "",
    loop_status: str,
    delivery_smoke_status: str,
    replies: int,
    payments: int,
    checkout_clicks: int,
    active_autonomous_ready: bool,
    active_queue_ready: bool,
) -> dict[str, Any]:
    urgent_states = {
        "infrastructure_blocked",
        "paid_fulfillment",
        "checkout_to_payment",
        "reply_to_payment",
        "active_sample_execution_missed",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
        "outbound_window_underfilled",
    }
    followup_states = {
        "messy_notes_to_payment",
        "sample_to_notes",
    }
    unhealthy_loop = loop_status in {"disabled", "error", "stuck", "late"}
    if unhealthy_loop:
        return {
            "mode": "attention_required",
            "do_not_interrupt_user": False,
            "reason": f"money loop is {loop_status}",
        }
    if delivery_smoke_status == "error":
        return {
            "mode": "attention_required",
            "do_not_interrupt_user": False,
            "reason": "delivery smoke check is failing",
        }
    if state in urgent_states:
        return {
            "mode": "attention_required",
            "do_not_interrupt_user": False,
            "reason": next_action or state,
        }
    if state in followup_states:
        return {
            "mode": "autonomous_followup_due",
            "do_not_interrupt_user": True,
            "reason": state,
        }
    if replies > payments or checkout_clicks > payments:
        return {
            "mode": "attention_required",
            "do_not_interrupt_user": False,
            "reason": "real buyer signal is ahead of payments",
        }
    if active_autonomous_ready:
        return {
            "mode": "autonomous_sending_now",
            "do_not_interrupt_user": True,
            "reason": "send window is open and queued leads are ready",
        }
    if active_queue_ready:
        return {
            "mode": "out_of_loop_waiting",
            "do_not_interrupt_user": True,
            "reason": "queued leads are ready for the next send window",
        }
    return {
        "mode": "out_of_loop_monitoring",
        "do_not_interrupt_user": True,
        "reason": state or "no immediate human action",
    }


def _launch_readiness_contract(
    *,
    money_state: str,
    money_next_action: str,
    loop_status: str,
    delivery_smoke_status: str,
    revenue_ladder: dict[str, Any],
    active_sends: int,
    active_target: int,
    active_remaining: int,
    active_due: int,
    cap_remaining: int,
    next_window_send_capacity: int,
    sent_today: int,
    sample_windows_to_complete: int,
    next_window: str | None,
    send_window_start: str | None,
    send_window_end: str | None,
    send_window_reason: str | None,
    experiment_decision_state: str,
    experiment_decision_next: str,
    active_signal_replies: int,
    active_signal_payments: int,
    unhandled_replies: int,
) -> dict[str, Any]:
    blockers: list[str] = []
    execution_blocker_states = {
        "infrastructure_blocked",
        "active_sample_execution_missed",
        "outbound_send_failed",
        "outbound_send_stalled",
        "outbound_window_missed",
        "outbound_window_underfilled",
    }
    if money_state in execution_blocker_states:
        blockers.append(money_next_action or money_state)
    if loop_status in {"disabled", "error", "stuck", "late"}:
        blockers.append(f"money loop is {loop_status}")
    if delivery_smoke_status == "error":
        blockers.append("delivery smoke check is failing")
    if not revenue_ladder.get("entry_offer_ready"):
        blockers.append("entry checkout link is not configured")
    if active_target <= 0:
        blockers.append("active experiment sample target is missing")
    if active_remaining > 0 and active_due <= 0:
        blockers.append("active experiment needs more queued first-touch leads")
    if active_remaining > 0 and sample_windows_to_complete <= 0:
        blockers.append("active experiment has no estimated send window capacity")
    if active_remaining > 0 and not str(next_window or "").strip():
        blockers.append("next send window is not known")

    if active_signal_payments > 0:
        interrupt_rule = "interrupt for fulfillment and keep the winning lane stable"
    elif unhandled_replies > 0:
        interrupt_rule = "interrupt to close unhandled replies through the paid next step"
    else:
        interrupt_rule = "do not interrupt unless unhandled replies, checkout/payment signal, or system health changes"

    if active_remaining > 0:
        proof_target = f"collect {active_remaining} more active-variant sends"
        success_metric = "first real reply, checkout/payment signal, or completed active sample"
    else:
        proof_target = "judge the completed active sample"
        success_metric = "active replies/payments decide whether to keep stable or rotate one variable"

    expected_next_window_sends = (
        min(active_remaining, active_due, next_window_send_capacity)
        if active_remaining > 0 and active_due > 0 and next_window_send_capacity > 0
        else 0
    )
    expected_sends_after_next_window = min(active_sends + expected_next_window_sends, active_target) if active_target else active_sends
    expected_progress_after_next_window = (
        f"{expected_sends_after_next_window}/{active_target}"
        if active_target
        else ""
    )
    if active_remaining > 0 and expected_next_window_sends > 0:
        next_window_success_criterion = (
            f"send {expected_next_window_sends} active leads and move progress "
            f"from {active_sends}/{active_target} to {expected_progress_after_next_window}"
        )
    elif active_remaining > 0 and cap_remaining <= 0 and active_due > 0:
        next_window_success_criterion = "wait for daily send capacity to reset"
    elif active_remaining > 0:
        next_window_success_criterion = "make queued active leads and send capacity available"
    else:
        next_window_success_criterion = "review the completed active sample and keep or rotate one variable"

    next_window_audit_at = _next_window_audit_at(
        next_window=next_window,
        send_window_start=send_window_start,
        send_window_end=send_window_end,
        send_window_reason=send_window_reason,
    )
    window_execution_state = _window_execution_state(
        send_window_reason=send_window_reason,
        sent_today=sent_today,
        cap_remaining=cap_remaining,
        active_due=active_due,
        active_remaining=active_remaining,
        expected_next_window_sends=expected_next_window_sends,
    )
    if expected_next_window_sends > 0:
        window_execution_failure_condition = (
            f"after audit time, interrupt if fewer than {expected_next_window_sends} active sends completed "
            "or the window closes with queued active leads and unused capacity"
        )
    elif cap_remaining <= 0 and active_due > 0:
        window_execution_failure_condition = "wait for daily send capacity to reset; interrupt only if the next fresh window still has no capacity"
    else:
        window_execution_failure_condition = "interrupt if the loop cannot create queued active leads or send capacity"

    readiness_state, readiness_reason = _launch_readiness_state(
        blockers=blockers,
        active_remaining=active_remaining,
        active_due=active_due,
        cap_remaining=cap_remaining,
        expected_next_window_sends=expected_next_window_sends,
        window_execution_state=window_execution_state,
    )

    return {
        "ready": not blockers,
        "readiness_state": readiness_state,
        "readiness_reason": readiness_reason,
        "blockers": blockers,
        "phase": experiment_decision_state,
        "proof_target": proof_target,
        "success_metric": success_metric,
        "review_rule": "do not judge the offer until the active sample is complete and its reply window has matured, or real buyer signal appears",
        "interrupt_rule": interrupt_rule,
        "next_decision": experiment_decision_next,
        "active_experiment_progress": f"{active_sends}/{active_target}" if active_target else "",
        "active_experiment_sends_remaining": active_remaining,
        "active_experiment_due_now": active_due,
        "expected_next_window_sends": expected_next_window_sends,
        "expected_progress_after_next_window": expected_progress_after_next_window,
        "next_window_success_criterion": next_window_success_criterion,
        "next_window_audit_at": next_window_audit_at,
        "window_execution_state": window_execution_state,
        "window_execution_failure_condition": window_execution_failure_condition,
        "window_execution_contract": {
            "state": window_execution_state,
            "expected_sends": expected_next_window_sends,
            "expected_progress": expected_progress_after_next_window,
            "audit_at": next_window_audit_at,
            "success_criterion": next_window_success_criterion,
            "failure_condition": window_execution_failure_condition,
        },
        "estimated_windows_remaining": sample_windows_to_complete,
        "next_autonomous_window": next_window,
    }


def _internal_emails() -> set[str]:
    configured = os.getenv("RELAY_INTERNAL_EMAILS", "pham.alann@gmail.com").split(",")
    return {email.strip().lower() for email in configured if email.strip()}


def _is_internal_email(email: str | None) -> bool:
    return (email or "").strip().lower() in _internal_emails()


def _sample_email_html(to_email: str) -> str:
    sample_url = _sample_packet_url()
    relay_url = _relay_url()
    checkout_url = settings.packet_checkout_url or "#"
    safe_email = escape(to_email)
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:620px">
      <p>Here is the Relay sample packet:</p>
      <p><a href="{sample_url}" style="color:#a05f2f;font-weight:700">Open the Relay sample PDF</a></p>
      <p>
        Relay turns rough call notes into client-ready follow-through:
        recap, next steps, follow-up, and CRM update.
      </p>
      <p>
        If you want the live one-call test, start here:
        <a href="{checkout_url}" style="color:#a05f2f;font-weight:700">Start the $40 relay</a>
      </p>
      <p style="font-size:13px;color:#756961">
        Sent to {safe_email} from <a href="{relay_url}" style="color:#756961">relaybrief.com</a>.
      </p>
    </div>
    """.strip()


def _send_sample_email(to_email: str) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=to_email,
            subject="Relay sample packet",
            html=_sample_email_html(to_email),
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=settings.reply_to_email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _send_messy_notes_email(payload: RelayIntentLeadIn, email: str, score: int) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    metadata = payload.metadata or {}
    notes = str(metadata.get("notes") or "").strip()[:5000]
    note_length = metadata.get("note_length") or len(notes)
    page_url = payload.page_url or _relay_url()
    safe_email = escape(email)
    safe_notes = escape(notes or "(No notes captured.)").replace("\n", "<br>")
    safe_page = escape(page_url)

    html = f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:720px">
      <p><strong>New Relay messy-notes submission</strong></p>
      <p><strong>Email:</strong> {safe_email}<br>
      <strong>Score:</strong> {score}<br>
      <strong>Note length:</strong> {escape(str(note_length))}</p>
      <div style="padding:14px 16px;border:1px solid #ead3c8;border-radius:12px;background:#fffaf6">
        {safe_notes}
      </div>
      <p style="font-size:13px;color:#756961">
        Source page: <a href="{safe_page}" style="color:#756961">{safe_page}</a>
      </p>
    </div>
    """.strip()

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=settings.reply_to_email,
            subject=f"New Relay notes from {email}",
            html=html,
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _messy_notes_customer_email_html(to_email: str) -> str:
    sample_url = _sample_packet_url()
    notes_url = _relay_url("/#send-notes")
    checkout_url = settings.packet_checkout_url or "#"
    safe_email = escape(to_email)
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#221b17;max-width:620px">
      <p>Got your Relay notes.</p>
      <p>
        The paid next step is the one-call packet. I turn one messy sales or client call
        into the recap, next steps, follow-up draft, open questions, and CRM-ready update.
      </p>
      <p>
        <a href="{checkout_url}" style="color:#a05f2f;font-weight:700">Start the $40 packet</a>
      </p>
      <p>
        Sample:
        <a href="{sample_url}" style="color:#a05f2f;font-weight:700">open the sample packet</a>
      </p>
      <p style="font-size:13px;color:#756961">
        If you need to resend or add detail, use <a href="{notes_url}" style="color:#756961">the notes form</a>.
        Sent to {safe_email}.
      </p>
    </div>
    """.strip()


def _send_messy_notes_customer_email(email: str) -> dict[str, Any]:
    if not settings.resend_api_key:
        return {"status": "skipped", "reason": "RESEND_API_KEY is not configured"}

    try:
        from app.integrations.resend_client import ResendClient

        response = ResendClient().send_email(
            to_email=email,
            subject="Got your Relay notes",
            html=_messy_notes_customer_email_html(email),
            from_email=settings.from_email_fulfillment or settings.from_email_outbound,
            reply_to=settings.reply_to_email,
        )
        return {
            "status": "sent",
            "provider": "resend",
            "provider_id": response.get("id") if isinstance(response, dict) else None,
        }
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)[:500]}


def _upsert_relay_acquisition_prospect(
    db,
    lead: RelayIntentLead,
    payload: RelayIntentLeadIn,
    email: str,
    source: str,
    score: int,
) -> dict[str, Any]:
    metadata = payload.metadata or {}
    source_lower = (source or "").lower()
    if "checkout_intent" in source_lower:
        inbound_stage = "checkout_intent"
        minimum_score = 90
    elif "messy_notes" in source_lower:
        inbound_stage = "messy_notes"
        minimum_score = 85
    elif "sample" in source_lower:
        inbound_stage = "sample_requested"
        minimum_score = 60
    else:
        inbound_stage = "inbound_lead"
        minimum_score = 40

    effective_score = max(int(score or 0), minimum_score)
    notes = str(metadata.get("notes") or "").strip()[:5000]
    prospect = (
        db.query(AcquisitionProspect)
        .filter(AcquisitionProspect.contact_email == email)
        .order_by(AcquisitionProspect.created_at.desc())
        .first()
    )

    created = False
    if prospect is None:
        prospect = AcquisitionProspect(
            external_id=f"relay-lead:{lead.id}",
            contact_email=email,
            company_name=str(metadata.get("company_name") or metadata.get("client_name") or "Relay lead")[:255],
            source="relay_intent",
            status="interested",
            fit_score=effective_score,
            fit_band="inbound",
            last_reply_state=inbound_stage,
        )
        db.add(prospect)
        created = True

    locked_paid_state = prospect.status in {"paid", "intake_received"} or prospect.stripe_status == "paid"
    if not locked_paid_state:
        prospect.status = "interested"
    prospect.source = prospect.source or "relay_intent"
    prospect.fit_score = max(int(prospect.fit_score or 0), effective_score)
    prospect.fit_band = prospect.fit_band or "inbound"
    if not locked_paid_state:
        prospect.last_reply_state = inbound_stage
    prospect.notes = notes or prospect.notes
    prospect.payload_json = json.dumps(
        {
            "relay_lead_id": lead.id,
            "session_id": lead.session_id,
            "source": source,
            "score": effective_score,
            "raw_score": score,
            "inbound_stage": inbound_stage,
            "metadata": metadata,
        },
        ensure_ascii=False,
    )[:8000]

    return {
        "status": "created" if created else "updated",
        "external_id": prospect.external_id,
        "contact_email": prospect.contact_email,
        "inbound_stage": inbound_stage,
        "fit_score": prospect.fit_score,
    }


def _user_agent(request: Request) -> str | None:
    ua = request.headers.get("user-agent")
    return ua[:1000] if ua else None


def _lead_score(session_id: str, source: str | None) -> int:
    db = SessionLocal()
    try:
        recent = datetime.utcnow() - timedelta(days=14)
        events = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.session_id == session_id)
            .filter(RelayIntentEvent.created_at >= recent)
            .group_by(RelayIntentEvent.event_type)
            .all()
        )
        counts = {name: int(count) for name, count in events}
        score = 10
        score += counts.get("sample_click", 0) * 8
        score += counts.get("checkout_click", 0) * 15
        score += counts.get("pricing_click", 0) * 12
        score += counts.get("client_gate_click", 0) * 10
        if source:
            score += 5
        return min(score, 100)
    finally:
        db.close()


def _latest_acquisition_event(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(AcquisitionEvent)
    for term in event_terms:
        query = query.filter(AcquisitionEvent.event_type.ilike(f"%{term}%"))
    event = query.order_by(AcquisitionEvent.created_at.desc()).first()
    if not event:
        return None
    return {
        "event_type": event.event_type,
        "summary": event.summary,
        "prospect_external_id": event.prospect_external_id,
        "created_at": event.created_at.isoformat(),
    }


def _latest_acquisition_payload(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(AcquisitionEvent)
    for term in event_terms:
        query = query.filter(AcquisitionEvent.event_type.ilike(f"%{term}%"))
    event = query.order_by(AcquisitionEvent.created_at.desc()).first()
    if not event:
        return None
    return _safe_payload(event.payload_json)


def _compact_money_loop_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not payload:
        return None
    refill = payload.get("refill_result") if isinstance(payload.get("refill_result"), dict) else {}
    fallback = refill.get("fallback_result") if isinstance(refill.get("fallback_result"), dict) else {}
    post_refill_outreach = (
        payload.get("post_refill_outreach_result")
        if isinstance(payload.get("post_refill_outreach_result"), dict)
        else {}
    )
    outreach = payload.get("outreach_result") if isinstance(payload.get("outreach_result"), dict) else {}
    success_control = payload.get("success_control") if isinstance(payload.get("success_control"), dict) else {}
    success_control_after = (
        payload.get("success_control_after_outreach")
        if isinstance(payload.get("success_control_after_outreach"), dict)
        else {}
    )
    success_conversion = (
        success_control.get("conversion_actions")
        if isinstance(success_control.get("conversion_actions"), dict)
        else {}
    )
    success_after_conversion = (
        success_control_after.get("conversion_actions")
        if isinstance(success_control_after.get("conversion_actions"), dict)
        else {}
    )
    status_after = payload.get("status_after") if isinstance(payload.get("status_after"), dict) else {}
    refill_backoff = (
        payload.get("refill_timeout_backoff")
        if isinstance(payload.get("refill_timeout_backoff"), dict)
        else {}
    )
    send_window_ready = (
        payload.get("send_window_ready_without_refill")
        if isinstance(payload.get("send_window_ready_without_refill"), dict)
        else {}
    )
    return {
        "refill_status": refill.get("status"),
        "refill_reason": refill.get("reason"),
        "refill_error_type": refill.get("error_type"),
        "refill_http_status": refill.get("http_status"),
        "refill_timeout_seconds": refill.get("timeout_seconds"),
        "apollo_primary_status": refill.get("apollo_primary_status"),
        "apollo_fallback_status": refill.get("apollo_fallback_status"),
        "refill_error_present": bool(refill.get("error")),
        "refill_source": refill.get("source"),
        "refill_query": refill.get("q_keywords"),
        "refill_searched": refill.get("searched"),
        "refill_enriched_with_email": refill.get("enriched_with_email"),
        "refill_missing_email_after_enrichment": refill.get("missing_email_after_enrichment"),
        "refill_skipped_missing_email": refill.get("skipped_missing_email"),
        "refill_upserted": refill.get("upserted"),
        "refill_prospects_with_email": refill.get("prospects_with_email"),
        "refill_sendable_upserted": refill.get("sendable_upserted"),
        "refill_direct_sendable_upserted": refill.get("direct_sendable_upserted"),
        "refill_generic_sendable_upserted": refill.get("generic_sendable_upserted"),
        "refill_missing_email_count": refill.get("missing_email_count"),
        "refill_rejected_or_unsendable_count": refill.get("rejected_or_unsendable_count"),
        "refill_capacity_mode": refill.get("refill_capacity_mode"),
        "refill_capacity_before": refill.get("refill_capacity_before"),
        "refill_capacity_after": refill.get("refill_capacity_after"),
        "refill_capacity_delta": refill.get("refill_capacity_delta"),
        "apollo_endpoint": refill.get("apollo_endpoint"),
        "apollo_primary_error_status": refill.get("apollo_primary_error_status"),
        "apollo_enrich_errors": refill.get("enrich_errors"),
        "refill_attempts": refill.get("attempts"),
        "fallback_status": fallback.get("status"),
        "fallback_error_type": fallback.get("error_type"),
        "fallback_timeout_seconds": fallback.get("timeout_seconds"),
        "fallback_upserted": fallback.get("upserted"),
        "fallback_sendable_upserted": fallback.get("sendable_upserted"),
        "fallback_direct_sendable_upserted": fallback.get("direct_sendable_upserted"),
        "fallback_generic_sendable_upserted": fallback.get("generic_sendable_upserted"),
        "fallback_capacity_delta": fallback.get("refill_capacity_delta"),
        "fallback_attempts": refill.get("fallback_attempts"),
        "fallback_searched": fallback.get("searched"),
        "active_experiment_needs_sample": payload.get("active_experiment_needs_sample"),
        "active_sample_sends_before": payload.get("active_sample_sends_before"),
        "active_sample_sends_after": payload.get("active_sample_sends_after"),
        "active_sample_delta_this_tick": payload.get("active_sample_delta_this_tick"),
        "active_sample_expected_delta_this_tick": payload.get("active_sample_expected_delta_this_tick"),
        "active_sample_tick_proof_state": payload.get("active_sample_tick_proof_state"),
        "active_sample_target": payload.get("active_sample_target"),
        "active_sample_progress_after": payload.get("active_sample_progress_after"),
        "active_experiment_new_due_before": payload.get("active_experiment_new_due_before"),
        "refill_due_before": payload.get("refill_due_before"),
        "refill_due_for_decision": payload.get("refill_due_for_decision"),
        "refill_due_target": payload.get("refill_due_target"),
        "active_sample_understocked": payload.get("active_sample_understocked"),
        "refill_backoff_active": refill_backoff.get("active"),
        "refill_backoff_reason": refill_backoff.get("timeout_reason") or refill_backoff.get("reason"),
        "refill_backoff_age_seconds": refill_backoff.get("age_seconds"),
        "refill_backoff_remaining_seconds": refill_backoff.get("remaining_seconds"),
        "send_window_ready_without_refill": send_window_ready.get("active"),
        "send_window_ready_reason": send_window_ready.get("reason"),
        "send_window_ready_needed_for_window": send_window_ready.get("needed_for_window"),
        "send_window_ready_active_needed_for_window": send_window_ready.get("active_needed_for_window"),
        "send_window_ready_active_buffer_windows": send_window_ready.get("active_sample_buffer_windows"),
        "send_window_ready_active_buffer_target": send_window_ready.get("active_sample_buffer_target"),
        "send_window_ready_active_sample_remaining": send_window_ready.get("active_experiment_sample_remaining"),
        "send_window_ready_next_open_local": send_window_ready.get("send_window_next_open_local"),
        "success_control_status_refreshed": payload.get("success_control_status_refreshed"),
        "success_control_refresh_reason": payload.get("success_control_refresh_reason"),
        "direct_due_before": payload.get("direct_due_before"),
        "send_window_open_before": payload.get("send_window_open_before"),
        "outreach_phase": payload.get("outreach_phase"),
        "sent_this_tick": payload.get("sent_this_tick"),
        "outreach_sent": outreach.get("send_result", {}).get("sent_count")
        if isinstance(outreach.get("send_result"), dict)
        else outreach.get("sent_count"),
        "outreach_summary": outreach.get("send_result", {}).get("summary")
        if isinstance(outreach.get("send_result"), dict)
        else outreach.get("summary"),
        "post_refill_outreach_sent": post_refill_outreach.get("send_result", {}).get("sent_count")
        if isinstance(post_refill_outreach.get("send_result"), dict)
        else None,
        "post_refill_outreach_summary": post_refill_outreach.get("send_result", {}).get("summary")
        if isinstance(post_refill_outreach.get("send_result"), dict)
        else None,
        "success_control_status": success_control.get("status"),
        "success_control_reason": success_control.get("reason"),
        "success_control_error_type": success_control.get("error_type"),
        "success_control_error": success_control.get("error"),
        "success_control_action_failures": success_control.get("action_failures"),
        "success_control_bottleneck": success_control.get("bottleneck"),
        "success_control_post_bottleneck": success_control.get("after_bottleneck"),
        "success_control_bottleneck_changed": success_control.get("bottleneck_changed"),
        "success_control_conversion_sent": success_conversion.get("sent_count"),
        "success_control_conversion_failures": success_conversion.get("failure_count"),
        "success_control_after_status": success_control_after.get("status"),
        "success_control_after_reason": success_control_after.get("reason"),
        "success_control_after_error_type": success_control_after.get("error_type"),
        "success_control_after_error": success_control_after.get("error"),
        "success_control_after_action_failures": success_control_after.get("action_failures"),
        "success_control_after_bottleneck": success_control_after.get("bottleneck"),
        "success_control_after_post_bottleneck": success_control_after.get("after_bottleneck"),
        "success_control_after_bottleneck_changed": success_control_after.get("bottleneck_changed"),
        "success_control_after_conversion_sent": success_after_conversion.get("sent_count"),
        "success_control_after_conversion_failures": success_after_conversion.get("failure_count"),
        "success_control_phase": payload.get("success_control_phase"),
        "success_control_after_reason": payload.get("success_control_after_reason"),
        "status_after": {
            "active_experiment_variant": status_after.get("active_experiment_variant"),
            "active_experiment_sends": status_after.get("active_experiment_sends"),
            "active_experiment_sample_target": status_after.get("active_experiment_sample_target"),
            "active_experiment_needs_sample": status_after.get("active_experiment_needs_sample"),
            "active_experiment_new_due_count": status_after.get("active_experiment_new_due_count"),
            "active_experiment_direct_new_due_count": status_after.get("active_experiment_direct_new_due_count"),
            "active_experiment_generic_new_due_count": status_after.get("active_experiment_generic_new_due_count"),
            "active_experiment_allowed_generic_new_due_count": status_after.get("active_experiment_allowed_generic_new_due_count"),
            "active_experiment_generic_sample_daily_cap": status_after.get("active_experiment_generic_sample_daily_cap"),
            "direct_due_count": status_after.get("direct_due_count"),
            "cap_remaining": status_after.get("cap_remaining"),
            "send_window_reason": status_after.get("send_window_reason"),
            "send_window_start_local": status_after.get("send_window_start_local"),
            "send_window_end_local": status_after.get("send_window_end_local"),
            "send_window_next_open_local": status_after.get("send_window_next_open_local"),
            "send_window_seconds_until_open": status_after.get("send_window_seconds_until_open"),
            "send_window_seconds_open": status_after.get("send_window_seconds_open"),
            "next_money_move": status_after.get("next_money_move"),
        },
    }


def _current_money_loop_runtime() -> dict[str, Any]:
    try:
        from app.services import relay_recovery_patch

        state = dict(getattr(relay_recovery_patch, "_money_loop_state", {}) or {})
        try:
            tick_timeout_seconds = int(relay_recovery_patch._money_loop_tick_timeout_seconds())
        except Exception:
            tick_timeout_seconds = int(float(os.getenv("AO_RELAY_MONEY_LOOP_TICK_TIMEOUT_SECONDS", "300") or 300))
    except Exception as exc:
        return {"status": "error", "summary": f"money_loop_state_unavailable:{type(exc).__name__}"}

    last_result = state.get("last_result") if isinstance(state.get("last_result"), dict) else None
    manual_result = state.get("last_manual_result") if isinstance(state.get("last_manual_result"), dict) else None
    running_seconds = None
    last_tick_at = state.get("last_tick_at") or ""
    last_tick_age_seconds = None
    if last_tick_at:
        try:
            started = datetime.fromisoformat(str(last_tick_at).replace("Z", "+00:00"))
            if started.tzinfo is not None:
                started = started.astimezone().replace(tzinfo=None)
            last_tick_age_seconds = max(int((datetime.now() - started).total_seconds()), 0)
        except Exception:
            last_tick_age_seconds = None

    if state.get("running"):
        running_seconds = last_tick_age_seconds

    next_sleep_raw = state.get("next_sleep_seconds")
    try:
        next_sleep_seconds = int(next_sleep_raw) if next_sleep_raw is not None else None
    except Exception:
        next_sleep_seconds = None
    running_stuck_after_seconds = tick_timeout_seconds + 60
    late_wake_after_seconds = (
        next_sleep_seconds + tick_timeout_seconds + 120
        if next_sleep_seconds is not None
        else None
    )
    enabled = bool(state.get("enabled"))
    running = bool(state.get("running"))
    last_error = state.get("last_error") or ""
    ticks = int(state.get("ticks") or 0)

    status = "ok"
    summary = "money_loop_ok"
    if not enabled:
        status = "disabled"
        summary = "money_loop_disabled"
    elif last_error:
        status = "error"
        summary = str(last_error)[:200]
    elif running and running_seconds is not None and running_seconds > running_stuck_after_seconds:
        status = "stuck"
        summary = "money_loop_tick_exceeded_timeout"
    elif ticks <= 0:
        status = "starting"
        summary = "money_loop_waiting_for_first_tick"
    elif (
        not running
        and last_tick_age_seconds is not None
        and late_wake_after_seconds is not None
        and last_tick_age_seconds > late_wake_after_seconds
    ):
        status = "late"
        summary = "money_loop_late_to_wake"

    return {
        "status": status,
        "summary": summary,
        "enabled": enabled,
        "running": running,
        "last_tick_at": last_tick_at,
        "last_tick_age_seconds": last_tick_age_seconds,
        "running_seconds": running_seconds,
        "tick_timeout_seconds": tick_timeout_seconds,
        "running_stuck_after_seconds": running_stuck_after_seconds,
        "late_wake_after_seconds": late_wake_after_seconds,
        "last_error": last_error,
        "next_sleep_seconds": next_sleep_seconds,
        "next_wake_reason": state.get("next_wake_reason") or "",
        "ticks": ticks,
        "last_manual_kick_at": state.get("last_manual_kick_at") or "",
        "last_result": _compact_money_loop_payload(last_result),
        "last_manual_result": _compact_money_loop_payload(manual_result),
    }


def _latest_production_transition(db, *event_terms: str) -> dict[str, Any] | None:
    query = db.query(ProductionTransition)
    for term in event_terms:
        query = query.filter(ProductionTransition.event_type.ilike(f"%{term}%"))
    event = query.order_by(ProductionTransition.created_at.desc()).first()
    if not event:
        return None
    return {
        "event_type": event.event_type,
        "summary": event.summary,
        "entity_external_id": event.entity_external_id,
        "old_state": event.old_state,
        "new_state": event.new_state,
        "created_at": event.created_at.isoformat(),
    }


def _latest_action(db, *action_terms: str) -> dict[str, Any] | None:
    query = db.query(ProductionAction)
    for term in action_terms:
        query = query.filter(ProductionAction.action_type.ilike(f"%{term}%"))
    action = query.order_by(ProductionAction.created_at.desc()).first()
    if not action:
        return None
    return {
        "action_type": action.action_type,
        "status": action.status,
        "to_email": action.to_email,
        "subject": action.subject,
        "created_at": action.created_at.isoformat(),
        "updated_at": action.updated_at.isoformat() if action.updated_at else None,
    }


def _latest_lead(
    db,
    source_term: str | None = None,
    *,
    exclude_internal: bool = False,
) -> dict[str, Any] | None:
    query = db.query(RelayIntentLead)
    if source_term:
        query = query.filter(RelayIntentLead.source.ilike(f"%{source_term}%"))
    if exclude_internal:
        query = query.filter(RelayIntentLead.email.notin_(_internal_emails()))

    lead = query.order_by(RelayIntentLead.created_at.desc()).first()
    if not lead:
        return None
    return {
        "email": lead.email,
        "source": lead.source,
        "score": lead.score,
        "session_id": lead.session_id,
        "created_at": lead.created_at.isoformat(),
    }


def _env_present(name: str) -> bool:
    return bool(str(os.getenv(name, "")).strip())


def _ready_label(checks: dict[str, Any]) -> str:
    missing = []
    env = checks.get("env", {})
    if not env.get("DATABASE_URL"):
        missing.append("DATABASE_URL")
    if not env.get("RESEND_API_KEY"):
        missing.append("RESEND_API_KEY")
    if not env.get("STRIPE_WEBHOOK_SECRET"):
        missing.append("STRIPE_WEBHOOK_SECRET")
    if not env.get("TALLY_WEBHOOK_SECRET"):
        missing.append("TALLY_WEBHOOK_SECRET")
    if not env.get("PACKET_CHECKOUT_URL"):
        missing.append("PACKET_CHECKOUT_URL")

    if missing:
        return "needs_env: " + ", ".join(missing)

    money_loop = checks.get("money_loop_runtime", {})
    money_loop_status = str(money_loop.get("status") or "unknown")
    if money_loop_status in {"disabled", "error", "stuck", "late"}:
        return f"money_loop_unhealthy:{money_loop_status}"

    recent = checks.get("recent", {})
    delivery_smoke = recent.get("last_delivery_smoke_detail")
    if isinstance(delivery_smoke, dict) and delivery_smoke.get("status") == "error":
        return "delivery_smoke_unhealthy"

    if not recent.get("last_stripe_event"):
        return "needs_stripe_live_test"
    if not recent.get("last_tally_event") and not recent.get("last_paid_relay_notes_fulfillment"):
        if recent.get("last_intake_smoke_test"):
            return "intake_smoke_ready_needs_paid_buyer"
        if recent.get("last_real_notes_intake_lead") or recent.get("last_notes_intake_lead"):
            return "notes_intake_ready_needs_paid_buyer"
        return "needs_intake_live_test"
    if not recent.get("last_delivery_or_sent_action") and not recent.get("last_paid_relay_notes_fulfillment"):
        return "needs_delivery_live_test"
    return "ready_or_nearly_ready"


@router.post("/event")
def record_relay_event(payload: RelayIntentEventIn, request: Request) -> dict[str, Any]:
    sid = _session_id(payload.session_id)
    event_type = payload.event_type.strip().lower().replace(" ", "_")[:80]

    db = SessionLocal()
    try:
        event = RelayIntentEvent(
            session_id=sid,
            event_type=event_type,
            path=(payload.path or "")[:512] or None,
            page_url=(payload.page_url or "")[:1000] or None,
            target_text=(payload.target_text or "")[:500] or None,
            target_href=(payload.target_href or "")[:1000] or None,
            referrer=(payload.referrer or "")[:1000] or None,
            user_agent=_user_agent(request),
            metadata_json=_json(payload.metadata),
        )
        db.add(event)
        db.commit()
        db.refresh(event)
        return {"ok": True, "event_id": event.id, "session_id": sid}
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"could not record relay event: {exc}") from exc
    finally:
        db.close()


@router.post("/lead")
def record_relay_lead(payload: RelayIntentLeadIn, request: Request) -> dict[str, Any]:
    sid = _session_id(payload.session_id)
    email = payload.email.strip().lower()

    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="valid email required")

    source = (payload.source or "sample_request").strip()[:120]
    score = _lead_score(sid, source)

    db = SessionLocal()
    try:
        lead = RelayIntentLead(
            session_id=sid,
            email=email,
            source=source,
            page_url=(payload.page_url or "")[:1000] or None,
            referrer=(payload.referrer or "")[:1000] or None,
            user_agent=_user_agent(request),
            score=score,
            metadata_json=_json(payload.metadata),
        )
        db.add(lead)

        db.add(
            RelayIntentEvent(
                session_id=sid,
                event_type="lead_capture",
                path=None,
                page_url=(payload.page_url or "")[:1000] or None,
                target_text=email,
                target_href=None,
                referrer=(payload.referrer or "")[:1000] or None,
                user_agent=_user_agent(request),
                metadata_json=_json({"source": source, "score": score}),
            )
        )

        db.commit()
        db.refresh(lead)

        source_lower = source.lower()
        acquisition_prospect = (
            _upsert_relay_acquisition_prospect(db, lead, payload, email, source, score)
            if any(term in source_lower for term in ["messy_notes", "sample", "checkout_intent"]) and not _is_internal_email(email)
            else {
                "status": "skipped",
                "reason": "internal test email" if _is_internal_email(email) else "not an inbound buyer signal",
            }
        )
        db.commit()

        sample_email = (
            _send_sample_email(email)
            if "sample" in source_lower
            else {"status": "skipped", "reason": "not a sample request"}
        )
        operator_email = (
            _send_messy_notes_email(payload, email, score)
            if "messy_notes" in source_lower
            else {"status": "skipped", "reason": "not a messy notes request"}
        )
        customer_email = (
            _send_messy_notes_customer_email(email)
            if "messy_notes" in source_lower
            else {"status": "skipped", "reason": "not a messy notes request"}
        )
        try:
            if acquisition_prospect.get("status") in {"created", "updated"}:
                db.add(
                    AcquisitionEvent(
                        event_type=f"relay_inbound_prospect_{acquisition_prospect.get('status', 'unknown')}",
                        prospect_external_id=str(acquisition_prospect.get("external_id") or f"relay-lead:{lead.id}"),
                        summary=(
                            f"inbound {acquisition_prospect.get('inbound_stage', 'lead')} prospect "
                            f"{acquisition_prospect.get('status', 'unknown')} for {email}"
                        ),
                        payload_json=json.dumps(
                            {
                                "lead_id": lead.id,
                                "session_id": sid,
                                "email": email,
                                "source": source,
                                "score": score,
                                "acquisition_prospect": acquisition_prospect,
                            },
                            ensure_ascii=False,
                        ),
                    )
                )
            db.add(
                AcquisitionEvent(
                    event_type=f"relay_sample_email_{sample_email.get('status', 'unknown')}",
                    prospect_external_id=f"relay-lead:{lead.id}",
                    summary=f"sample email {sample_email.get('status', 'unknown')} for {email}",
                    payload_json=json.dumps(
                        {
                            "lead_id": lead.id,
                            "session_id": sid,
                            "email": email,
                            "source": source,
                            "score": score,
                            "acquisition_prospect": acquisition_prospect,
                            "sample_email": sample_email,
                            "operator_email": operator_email,
                            "customer_email": customer_email,
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            if "messy_notes" in source_lower:
                db.add(
                    AcquisitionEvent(
                        event_type=f"relay_messy_notes_email_{operator_email.get('status', 'unknown')}",
                        prospect_external_id=f"relay-lead:{lead.id}",
                        summary=f"messy notes email {operator_email.get('status', 'unknown')} for {email}",
                        payload_json=json.dumps(
                            {
                                "lead_id": lead.id,
                                "session_id": sid,
                                "email": email,
                                "source": source,
                                "score": score,
                                "acquisition_prospect": acquisition_prospect,
                                "operator_email": operator_email,
                                "customer_email": customer_email,
                            },
                            ensure_ascii=False,
                        ),
                    )
                )
            db.commit()
        except Exception:
            db.rollback()

        return {
            "ok": True,
            "lead_id": lead.id,
            "session_id": sid,
            "score": score,
            "sample_email": sample_email,
            "operator_email": operator_email,
            "customer_email": customer_email,
            "acquisition_prospect": acquisition_prospect,
        }
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"could not record relay lead: {exc}") from exc
    finally:
        db.close()


@router.get("/intent-summary")
def relay_intent_summary(days: int = 7, limit: int = 20) -> dict[str, Any]:
    days = max(1, min(days, 90))
    limit = max(1, min(limit, 100))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        event_counts = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.event_type)
            .order_by(func.count(RelayIntentEvent.id).desc())
            .all()
        )

        leads = (
            db.query(RelayIntentLead)
            .filter(RelayIntentLead.created_at >= since)
            .order_by(RelayIntentLead.created_at.desc())
            .limit(limit)
            .all()
        )

        hot_sessions = (
            db.query(RelayIntentEvent.session_id, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.session_id)
            .order_by(func.count(RelayIntentEvent.id).desc())
            .limit(limit)
            .all()
        )

        return {
            "ok": True,
            "days": days,
            "event_counts": [{"event_type": name, "count": int(count)} for name, count in event_counts],
            "leads": [
                {
                    "email": lead.email,
                    "source": lead.source,
                    "score": lead.score,
                    "created_at": lead.created_at.isoformat(),
                    "session_id": lead.session_id,
                }
                for lead in leads
            ],
            "hot_sessions": [
                {"session_id": session_id, "event_count": int(count)}
                for session_id, count in hot_sessions
            ],
        }
    finally:
        db.close()


@router.get("/research-journal")
def relay_research_journal(days: int = 30, limit: int = 50) -> dict[str, Any]:
    days = max(1, min(days, 365))
    limit = max(1, min(limit, 200))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        total_count = (
            db.query(func.count(AcquisitionEvent.id))
            .filter(AcquisitionEvent.event_type == RELAY_RESEARCH_JOURNAL_EVENT)
            .filter(AcquisitionEvent.created_at >= since)
            .scalar()
            or 0
        )
        events = (
            db.query(AcquisitionEvent)
            .filter(AcquisitionEvent.event_type == RELAY_RESEARCH_JOURNAL_EVENT)
            .filter(AcquisitionEvent.created_at >= since)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "ok": True,
            "days": days,
            "limit": limit,
            "event_type": RELAY_RESEARCH_JOURNAL_EVENT,
            "count": int(total_count),
            "entries": [
                {
                    "id": event.id,
                    "created_at": event.created_at.isoformat() if event.created_at else "",
                    "source": event.prospect_external_id.replace("relay-research:", "", 1),
                    "summary": event.summary,
                    "payload": _safe_payload(event.payload_json),
                }
                for event in events
            ],
        }
    finally:
        db.close()


@router.get("/decision-ledger")
def relay_decision_ledger(days: int = 30, limit: int = 50) -> dict[str, Any]:
    days = max(1, min(days, 365))
    limit = max(1, min(limit, 200))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        events = (
            db.query(AcquisitionEvent)
            .filter(AcquisitionEvent.event_type == RELAY_RESEARCH_JOURNAL_EVENT)
            .filter(AcquisitionEvent.created_at >= since)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
            .all()
        )
        entries = []
        for event in events:
            payload = _safe_payload(event.payload_json)
            ledger = payload.get("decision_ledger") if isinstance(payload, dict) else None
            if not isinstance(ledger, dict):
                continue
            entries.append(
                {
                    "id": event.id,
                    "created_at": event.created_at.isoformat() if event.created_at else "",
                    "source": event.prospect_external_id.replace("relay-research:", "", 1),
                    "summary": event.summary,
                    "ledger": ledger,
                }
            )
        return {
            "ok": True,
            "days": days,
            "limit": limit,
            "event_type": RELAY_RESEARCH_JOURNAL_EVENT,
            "count": len(entries),
            "entries": entries,
        }
    finally:
        db.close()


@router.get("/evidence-export")
def relay_evidence_export(days: int = 90, limit: int = 200) -> dict[str, Any]:
    days = max(1, min(days, 365))
    limit = max(1, min(limit, 1000))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        intent_rows = (
            db.query(func.date(RelayIntentEvent.created_at), RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(func.date(RelayIntentEvent.created_at), RelayIntentEvent.event_type)
            .order_by(func.date(RelayIntentEvent.created_at).asc(), RelayIntentEvent.event_type.asc())
            .all()
        )
        lead_rows = (
            db.query(func.date(RelayIntentLead.created_at), RelayIntentLead.source, func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .group_by(func.date(RelayIntentLead.created_at), RelayIntentLead.source)
            .order_by(func.date(RelayIntentLead.created_at).asc(), RelayIntentLead.source.asc())
            .all()
        )
        acquisition_rows = (
            db.query(func.date(AcquisitionEvent.created_at), AcquisitionEvent.event_type, func.count(AcquisitionEvent.id))
            .filter(AcquisitionEvent.created_at >= since)
            .group_by(func.date(AcquisitionEvent.created_at), AcquisitionEvent.event_type)
            .order_by(func.date(AcquisitionEvent.created_at).asc(), AcquisitionEvent.event_type.asc())
            .all()
        )
        prospect_status_rows = (
            db.query(AcquisitionProspect.status, func.count(AcquisitionProspect.id))
            .group_by(AcquisitionProspect.status)
            .order_by(func.count(AcquisitionProspect.id).desc())
            .all()
        )
        prospect_reply_rows = (
            db.query(AcquisitionProspect.last_reply_state, func.count(AcquisitionProspect.id))
            .group_by(AcquisitionProspect.last_reply_state)
            .order_by(func.count(AcquisitionProspect.id).desc())
            .all()
        )
        intent_total_rows = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.event_type)
            .order_by(func.count(RelayIntentEvent.id).desc())
            .all()
        )
        acquisition_total_rows = (
            db.query(AcquisitionEvent.event_type, func.count(AcquisitionEvent.id))
            .filter(AcquisitionEvent.created_at >= since)
            .group_by(AcquisitionEvent.event_type)
            .order_by(func.count(AcquisitionEvent.id).desc())
            .all()
        )
        leads = (
            db.query(RelayIntentLead)
            .filter(RelayIntentLead.created_at >= since)
            .order_by(RelayIntentLead.created_at.desc())
            .limit(limit)
            .all()
        )
        journal_events = (
            db.query(AcquisitionEvent)
            .filter(AcquisitionEvent.event_type == RELAY_RESEARCH_JOURNAL_EVENT)
            .filter(AcquisitionEvent.created_at >= since)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
            .all()
        )

        def clean_day(value: Any) -> str:
            return value.isoformat() if hasattr(value, "isoformat") else str(value or "")

        def lead_domain(email: str | None) -> str:
            if not email or "@" not in email:
                return ""
            return email.rsplit("@", 1)[-1].strip().lower()[:255]

        journal_entries = []
        ledger_entries = []
        for event in journal_events:
            payload = _safe_payload(event.payload_json)
            source = event.prospect_external_id.replace("relay-research:", "", 1)
            journal_entries.append(
                {
                    "id": event.id,
                    "created_at": event.created_at.isoformat() if event.created_at else "",
                    "source": source,
                    "summary": event.summary,
                    "payload": payload,
                }
            )
            ledger = payload.get("decision_ledger") if isinstance(payload.get("decision_ledger"), dict) else {}
            if ledger:
                ledger_entries.append(
                    {
                        "id": event.id,
                        "created_at": event.created_at.isoformat() if event.created_at else "",
                        "source": source,
                        "summary": event.summary,
                        "ledger": ledger,
                    }
                )

        return {
            "ok": True,
            "schema_version": "relay-evidence-export-v1",
            "generated_at": datetime.utcnow().isoformat(),
            "window": {
                "days": days,
                "since": since.isoformat(),
                "limit": limit,
            },
            "privacy": {
                "lead_emails": "redacted_to_domain",
                "raw_notes": "not_exported",
            },
            "coverage": {
                "raw_product_events": "relay_intent_events",
                "raw_intent_leads": "relay_intent_leads",
                "outreach_replies_payments_and_journal": "acquisition_events",
                "decision_ledger_source": "research_journal_payload.decision_ledger",
            },
            "totals": {
                "intent_event_counts": {name or "unknown": int(count) for name, count in intent_total_rows},
                "acquisition_event_counts": {name or "unknown": int(count) for name, count in acquisition_total_rows},
                "prospect_status_counts": {status or "unknown": int(count) for status, count in prospect_status_rows},
                "prospect_reply_state_counts": {state or "unknown": int(count) for state, count in prospect_reply_rows},
            },
            "daily": {
                "intent_event_counts": [
                    {"date": clean_day(day), "event_type": name or "unknown", "count": int(count)}
                    for day, name, count in intent_rows
                ],
                "lead_counts": [
                    {"date": clean_day(day), "source": source or "unknown", "count": int(count)}
                    for day, source, count in lead_rows
                ],
                "acquisition_event_counts": [
                    {"date": clean_day(day), "event_type": name or "unknown", "count": int(count)}
                    for day, name, count in acquisition_rows
                ],
            },
            "leads": [
                {
                    "created_at": lead.created_at.isoformat() if lead.created_at else "",
                    "source": lead.source,
                    "score": int(lead.score or 0),
                    "email_domain": lead_domain(lead.email),
                    "session_id": lead.session_id,
                    "page_url": lead.page_url,
                    "metadata": _safe_payload(lead.metadata_json),
                }
                for lead in leads
            ],
            "research_journal": {
                "count": len(journal_entries),
                "entries": journal_entries,
            },
            "decision_ledger": {
                "count": len(ledger_entries),
                "entries": ledger_entries,
            },
        }
    finally:
        db.close()


@router.get("/ops-check")
def relay_ops_check(days: int = 14) -> dict[str, Any]:
    days = max(1, min(days, 90))
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        revenue_ladder = _revenue_ladder_status()
        env = {
            "DATABASE_URL": _env_present("DATABASE_URL"),
            "RESEND_API_KEY": _env_present("RESEND_API_KEY"),
            "STRIPE_SECRET_KEY": _env_present("STRIPE_SECRET_KEY"),
            "STRIPE_WEBHOOK_SECRET": _env_present("STRIPE_WEBHOOK_SECRET"),
            "TALLY_WEBHOOK_SECRET": _env_present("TALLY_WEBHOOK_SECRET"),
            "PACKET_CHECKOUT_URL": bool(getattr(settings, "packet_checkout_url", "") or os.getenv("PACKET_CHECKOUT_URL", "")),
            "PACKET_5_PACK_URL": bool(_configured_offer_url("PACKET_5_PACK_URL", "packet_5_pack_url")),
            "WEEKLY_SPRINT_URL": bool(_configured_offer_url("WEEKLY_SPRINT_URL", "weekly_sprint_url")),
            "MONTHLY_AUTOPILOT_URL": bool(_configured_offer_url("MONTHLY_AUTOPILOT_URL", "monthly_autopilot_url")),
            "CLIENT_INTAKE_DESTINATION": bool(getattr(settings, "client_intake_destination", "") or os.getenv("CLIENT_INTAKE_DESTINATION", "")),
            "FROM_EMAIL_FULFILLMENT": bool(getattr(settings, "from_email_fulfillment", "") or os.getenv("FROM_EMAIL_FULFILLMENT", "")),
        }

        route_surface = {
            "stripe_webhook_route": "/webhooks/stripe",
            "tally_webhook_route": "/webhooks/tally",
            "client_gate_route": "/client-gate/redeem",
            "production_event_route": "/production/event",
            "production_digest_route": "/production/digest",
            "autopilot_batch_route": "/autopilot/batch",
            "autopilot_digest_route": "/autopilot/digest",
            "intent_summary_route": "/api/relay/intent-summary",
            "research_journal_route": "/api/relay/research-journal",
            "decision_ledger_route": "/api/relay/decision-ledger",
        }

        event_counts = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
            .group_by(RelayIntentEvent.event_type)
            .all()
        )
        intent_counts = {name: int(count) for name, count in event_counts}
        internal_sessions = [
            session_id
            for (session_id,) in (
                db.query(RelayIntentLead.session_id)
                .filter(RelayIntentLead.created_at >= since)
                .filter(RelayIntentLead.email.in_(_internal_emails()))
                .all()
            )
            if session_id
        ]
        real_event_query = (
            db.query(RelayIntentEvent.event_type, func.count(RelayIntentEvent.id))
            .filter(RelayIntentEvent.created_at >= since)
        )
        if internal_sessions:
            real_event_query = real_event_query.filter(RelayIntentEvent.session_id.notin_(internal_sessions))
        real_event_counts = real_event_query.group_by(RelayIntentEvent.event_type).all()
        real_intent_counts = {name: int(count) for name, count in real_event_counts}

        lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).scalar() or 0
        hot_lead_count = db.query(func.count(RelayIntentLead.id)).filter(RelayIntentLead.created_at >= since).filter(RelayIntentLead.score >= 50).scalar() or 0
        real_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.notin_(_internal_emails()))
            .scalar()
            or 0
        )
        real_hot_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.notin_(_internal_emails()))
            .filter(RelayIntentLead.score >= 50)
            .scalar()
            or 0
        )
        internal_test_lead_count = (
            db.query(func.count(RelayIntentLead.id))
            .filter(RelayIntentLead.created_at >= since)
            .filter(RelayIntentLead.email.in_(_internal_emails()))
            .scalar()
            or 0
        )

        prospect_counts = (
            db.query(AcquisitionProspect.status, func.count(AcquisitionProspect.id))
            .group_by(AcquisitionProspect.status)
            .all()
        )
        stripe_paid_count = db.query(func.count(AcquisitionProspect.id)).filter(AcquisitionProspect.stripe_status == "paid").scalar() or 0
        intake_received_count = db.query(func.count(AcquisitionProspect.id)).filter(AcquisitionProspect.intake_status == "received").scalar() or 0

        production_lead_count = db.query(func.count(ProductionLead.id)).scalar() or 0
        production_opportunity_count = db.query(func.count(ProductionOpportunity.id)).scalar() or 0
        open_exception_count = db.query(func.count(ProductionException.id)).filter(ProductionException.resolved == False).scalar() or 0  # noqa: E712
        pending_action_count = db.query(func.count(ProductionAction.id)).filter(ProductionAction.status == "pending").scalar() or 0
        sent_action_count = db.query(func.count(ProductionAction.id)).filter(ProductionAction.status.in_(["sent", "completed", "done"])).scalar() or 0

        recent = {
            "last_stripe_event": _latest_acquisition_event(db, "stripe"),
            "last_tally_event": _latest_acquisition_event(db, "intake_received"),
            "last_intake_smoke_test": _latest_acquisition_event(db, "relay_intake_smoke_test"),
            "last_delivery_smoke_test": _latest_acquisition_event(db, "relay_delivery_smoke_test"),
            "last_delivery_smoke_detail": _latest_acquisition_payload(db, "relay_delivery_smoke_test"),
            "last_payment_or_paid_prospect": _latest_acquisition_event(db, "paid"),
            "last_success_control_tick": _latest_acquisition_event(db, "relay_success_control_tick"),
            "last_money_loop_tick": _latest_acquisition_event(db, "relay_money_loop_tick"),
            "last_money_loop_detail": _compact_money_loop_payload(_latest_acquisition_payload(db, "relay_money_loop_tick")),
            "last_research_journal_entry": _latest_acquisition_event(db, RELAY_RESEARCH_JOURNAL_EVENT),
            "last_outbound_experiment_plan": _latest_acquisition_event(db, "relay_experiment_plan"),
            "last_inbound_followup": _latest_acquisition_event(db, "autopilot_messy_notes_checkout_followup_sent")
            or _latest_acquisition_event(db, "autopilot_messy_notes_second_followup_sent")
            or _latest_acquisition_event(db, "autopilot_sample_notes_followup_sent")
            or _latest_acquisition_event(db, "autopilot_sample_second_followup_sent")
            or _latest_acquisition_event(db, "autopilot_checkout_intent_followup_sent")
            or _latest_acquisition_event(db, "autopilot_checkout_intent_second_followup_sent"),
            "last_inbound_prospect": _latest_acquisition_event(db, "relay_inbound_prospect"),
            "last_paid_relay_notes_fulfillment": _latest_acquisition_event(db, "autopilot_paid_relay_notes_fulfilled"),
            "last_production_transition": _latest_production_transition(db),
            "last_delivery_or_sent_action": _latest_action(db, "delivery") or _latest_action(db, "send") or _latest_action(db, "email"),
            "last_relay_intent_lead": _latest_lead(db),
            "last_notes_intake_lead": _latest_lead(db, "messy_notes"),
            "last_real_notes_intake_lead": _latest_lead(db, "messy_notes", exclude_internal=True),
            "last_checkout_intent_lead": _latest_lead(db, "checkout_intent", exclude_internal=True),
        }

        checks = {
            "ok": True,
            "days": days,
            "route_surface": route_surface,
            "env": env,
            "intent": {
                "event_counts": intent_counts,
                "lead_count": int(lead_count),
                "hot_lead_count": int(hot_lead_count),
                "real_event_counts": real_intent_counts,
                "real_lead_count": int(real_lead_count),
                "real_hot_lead_count": int(real_hot_lead_count),
                "internal_test_lead_count": int(internal_test_lead_count),
            },
            "acquisition": {
                "status_counts": {status or "unknown": int(count) for status, count in prospect_counts},
                "stripe_paid_count": int(stripe_paid_count),
                "intake_received_count": int(intake_received_count),
            },
            "production": {
                "lead_count": int(production_lead_count),
                "opportunity_count": int(production_opportunity_count),
                "pending_action_count": int(pending_action_count),
                "sent_action_count": int(sent_action_count),
                "open_exception_count": int(open_exception_count),
            },
            "recent": recent,
            "money_loop_runtime": _current_money_loop_runtime(),
        }
        try:
            from app.services.relay_performance import relay_performance_status
            from app.services.relay_success_controller import relay_success_status
            from app.services.custom_outreach import outreach_status

            performance = relay_performance_status()
            success = relay_success_status()
            success_snapshot = success.get("snapshot") or {}
            active_reply_observation = (
                success_snapshot.get("active_reply_observation")
                if isinstance(success_snapshot.get("active_reply_observation"), dict)
                else {}
            )
            active_experiment = performance.get("active_experiment") or {}
            outreach = outreach_status()
            active_sends = _safe_int(
                outreach.get("active_experiment_sample_sends") or outreach.get("active_experiment_sends")
            )
            active_target = _safe_int(outreach.get("active_experiment_sample_target"))
            active_due = _safe_int(outreach.get("active_experiment_new_due_count"))
            active_remaining = max(active_target - active_sends, 0) if active_target else 0
            active_signal = performance.get("active_experiment_signal") or {}
            active_signal_replies = _safe_int(active_signal.get("replies"))
            active_signal_payments = _safe_int(active_signal.get("payments"))
            if active_remaining > 0:
                experiment_decision_state = "collecting_sample"
                experiment_decision_next = "Keep sending until the active experiment reaches its sample target."
            elif active_signal_replies > 0 or active_signal_payments > 0:
                experiment_decision_state = "signal_exists_keep_stable"
                experiment_decision_next = "Keep the current lane stable and focus on closing real signal."
            elif bool(active_reply_observation.get("pending")):
                experiment_decision_state = "observing_reply_window"
                experiment_decision_next = "Wait through the active sample reply window before rotating copy or targeting."
            else:
                experiment_decision_state = "ready_to_review_or_rotate"
                experiment_decision_next = "Run the outbound experiment review and rotate one controlled variable."
            cap_remaining = _safe_int(outreach.get("cap_remaining"))
            effective_daily_cap = _safe_int(outreach.get("effective_daily_cap") or outreach.get("daily_send_cap"))
            current_window_cap = max(min(cap_remaining, effective_daily_cap or cap_remaining), 0)
            future_window_cap = max(effective_daily_cap, current_window_cap)
            send_window_open = bool(outreach.get("send_window_is_open"))
            next_window_cap = current_window_cap if send_window_open else future_window_cap
            send_capacity_per_window = min(active_due, current_window_cap) if active_remaining > 0 and active_due > 0 else 0
            next_window_send_capacity = (
                min(active_due, next_window_cap)
                if active_remaining > 0 and active_due > 0 and next_window_cap > 0
                else 0
            )
            estimated_capacity_per_future_window = (
                min(active_due, future_window_cap)
                if active_remaining > 0 and active_due > 0 and future_window_cap > 0
                else 0
            )
            sample_windows_to_complete = _ceil_div(active_remaining, estimated_capacity_per_future_window)
            queued_sample_covers_remaining = active_due >= active_remaining if active_remaining > 0 else True
            active_queue_ready = active_due > 0 and cap_remaining > 0
            active_autonomous_ready = active_queue_ready and send_window_open
            checks["relay_performance"] = {
                "active_experiment": {
                    "experiment_variant": active_experiment.get("experiment_variant"),
                    "experiment_label": active_experiment.get("experiment_label"),
                    "source": active_experiment.get("source"),
                    "created_at": active_experiment.get("created_at"),
                    "logged_at": active_experiment.get("logged_at"),
                    "week_start_date": active_experiment.get("week_start_date"),
                },
                "active_experiment_signal": active_signal,
                "active_experiment_signal_window": performance.get("active_experiment_signal_window") or {},
                "rolling_7_day": performance.get("rolling_7_day") or {},
                "active_experiment_queue": {
                    "active_experiment_variant": outreach.get("active_experiment_variant"),
                    "active_experiment_sends": active_sends,
                    "active_experiment_sample_target": active_target,
                    "active_experiment_sends_remaining": active_remaining,
                    "active_experiment_progress_label": f"{active_sends}/{active_target}" if active_target else "",
                    "active_experiment_needs_sample": outreach.get("active_experiment_needs_sample"),
                    "active_experiment_new_due_count": active_due,
                    "active_experiment_direct_new_due_count": outreach.get("active_experiment_direct_new_due_count"),
                    "active_experiment_generic_new_due_count": outreach.get("active_experiment_generic_new_due_count"),
                    "active_experiment_allowed_generic_new_due_count": outreach.get("active_experiment_allowed_generic_new_due_count"),
                    "active_experiment_generic_sample_daily_cap": outreach.get("active_experiment_generic_sample_daily_cap"),
                    "direct_due_count": outreach.get("direct_due_count"),
                    "cap_remaining": cap_remaining,
                    "effective_daily_cap": effective_daily_cap,
                    "active_experiment_send_capacity_per_window": send_capacity_per_window,
                    "active_experiment_windows_to_complete_at_current_cap": sample_windows_to_complete,
                    "active_experiment_queued_sample_covers_remaining": queued_sample_covers_remaining,
                    "active_experiment_next_window_send_capacity": next_window_send_capacity,
                    "send_window_is_open": send_window_open,
                    "send_window_reason": outreach.get("send_window_reason"),
                    "send_window_now_local": outreach.get("send_window_now_local"),
                    "send_window_start_local": outreach.get("send_window_start_local"),
                    "send_window_end_local": outreach.get("send_window_end_local"),
                    "send_window_next_open_local": outreach.get("send_window_next_open_local"),
                    "send_window_seconds_until_open": outreach.get("send_window_seconds_until_open"),
                    "send_window_seconds_open": outreach.get("send_window_seconds_open"),
                    "send_window_business_days_only": outreach.get("send_window_business_days_only"),
                    "active_experiment_queue_ready": active_queue_ready,
                    "active_experiment_autonomous_send_ready": active_autonomous_ready,
                    "next_money_move": outreach.get("next_money_move"),
                },
                "active_experiment_decision": {
                    "state": experiment_decision_state,
                    "next": experiment_decision_next,
                    "active_experiment_sends": active_sends,
                    "active_experiment_sample_target": active_target,
                    "active_experiment_sends_remaining": active_remaining,
                    "active_experiment_replies": active_signal_replies,
                    "active_experiment_payments": active_signal_payments,
                    "active_reply_observation": active_reply_observation,
                },
            }
            checks["relay_success"] = {
                "bottleneck": success.get("bottleneck"),
                "next_action": success.get("next_action"),
                "money_proof_mandate": success.get("money_proof_mandate") or {},
                "money_proof_health": success.get("money_proof_health") or {},
                "active_reply_observation": active_reply_observation,
                "money": success_snapshot.get("money") or {},
                "intent": success_snapshot.get("intent") or {},
                "outreach": success_snapshot.get("outreach") or {},
                "conversion": success_snapshot.get("conversion") or {},
                "outbound_preflight": success_snapshot.get("outbound_preflight") or {},
                "public_offer_preflight": success_snapshot.get("public_offer_preflight") or {},
                "reply_autoclose_preflight": success_snapshot.get("reply_autoclose_preflight") or {},
                "payment_webhook_preflight": success_snapshot.get("payment_webhook_preflight") or {},
                "critical_missing": success_snapshot.get("critical_missing") or [],
            }
            money = success_snapshot.get("money") or {}
            success_intent = success_snapshot.get("intent") or {}
            success_outreach = success_snapshot.get("outreach") or {}
            payments = _safe_int(money.get("payments"))
            replies = _safe_int(success_outreach.get("replies"))
            auto_replies = _safe_int(success_outreach.get("auto_replies"))
            unhandled_replies = _safe_int(
                success_outreach.get("unhandled_replies")
                if success_outreach.get("unhandled_replies") is not None
                else max(replies - auto_replies - payments, 0)
            )
            auto_closed_replies = min(auto_replies, replies)
            checkout_clicks = _safe_int(success_intent.get("checkout_clicks"))
            reply_autonomy_contract = _reply_autonomy_contract(
                replies=replies,
                auto_replies=auto_replies,
                unhandled_replies=unhandled_replies,
                checkout_clicks=checkout_clicks,
                payments=payments,
                reply_autoclose_mode=str(outreach.get("reply_autoclose_mode") or ""),
            )
            revenue_objective = _money_objective_status(
                money=money,
                outreach=outreach,
                payments=payments,
            )
            money_decision_contract = _money_decision_contract(
                active_sends=active_sends,
                active_target=active_target,
                active_remaining=active_remaining,
                active_signal_replies=active_signal_replies,
                active_signal_payments=active_signal_payments,
                auto_closed_replies=auto_closed_replies,
                unhandled_replies=unhandled_replies,
                checkout_clicks=checkout_clicks,
                payments=payments,
                experiment_decision_next=experiment_decision_next,
            )
            money_state = str(success.get("bottleneck") or "unknown")
            loop_status = str(checks.get("money_loop_runtime", {}).get("status") or "unknown")
            sent_today = _safe_int(outreach.get("sent_today"))
            latest_delivery_smoke = recent.get("last_delivery_smoke_detail")
            delivery_smoke_status = (
                str(latest_delivery_smoke.get("status") or "")
                if isinstance(latest_delivery_smoke, dict)
                else ""
            )
            operator_mode = _operator_mode(
                state=money_state,
                next_action=str(success.get("next_action") or ""),
                loop_status=loop_status,
                delivery_smoke_status=delivery_smoke_status,
                replies=unhandled_replies,
                payments=payments,
                checkout_clicks=checkout_clicks,
                active_autonomous_ready=active_autonomous_ready,
                active_queue_ready=active_queue_ready,
            )
            launch_readiness = _launch_readiness_contract(
                money_state=money_state,
                money_next_action=str(success.get("next_action") or ""),
                loop_status=loop_status,
                delivery_smoke_status=delivery_smoke_status,
                revenue_ladder=revenue_ladder,
                active_sends=active_sends,
                active_target=active_target,
                active_remaining=active_remaining,
                active_due=active_due,
                cap_remaining=cap_remaining,
                next_window_send_capacity=next_window_send_capacity,
                sent_today=sent_today,
                sample_windows_to_complete=sample_windows_to_complete,
                next_window=outreach.get("send_window_next_open_local"),
                send_window_start=outreach.get("send_window_start_local"),
                send_window_end=outreach.get("send_window_end_local"),
                send_window_reason=outreach.get("send_window_reason"),
                experiment_decision_state=experiment_decision_state,
                experiment_decision_next=experiment_decision_next,
                active_signal_replies=active_signal_replies,
                active_signal_payments=active_signal_payments,
                unhandled_replies=unhandled_replies,
            )
            conversion_ladder = _conversion_ladder_contract(
                sends=_safe_int(success_outreach.get("sends")),
                active_sends=active_sends,
                active_target=active_target,
                active_remaining=active_remaining,
                replies=replies,
                auto_replies=auto_replies,
                unhandled_replies=unhandled_replies,
                checkout_clicks=checkout_clicks,
                payments=payments,
                revenue_objective=revenue_objective,
                launch_readiness=launch_readiness,
            )
            success_governor = _success_governor_contract(
                revenue_objective=revenue_objective,
                money_decision=money_decision_contract,
                reply_autonomy=reply_autonomy_contract,
                launch_readiness=launch_readiness,
                operator_mode=operator_mode,
            )
            owner_absence_contract = _owner_absence_contract(
                success_governor=success_governor,
                launch_readiness=launch_readiness,
                reply_autonomy=reply_autonomy_contract,
                conversion_ladder=conversion_ladder,
                revenue_objective=revenue_objective,
            )
            autonomous_money_mandate = _autonomous_money_mandate(
                revenue_objective=revenue_objective,
                money_decision=money_decision_contract,
                reply_autonomy=reply_autonomy_contract,
                conversion_ladder=conversion_ladder,
                success_governor=success_governor,
                owner_absence=owner_absence_contract,
                launch_readiness=launch_readiness,
            )
            money_proof_mandate = success.get("money_proof_mandate") or {}
            money_proof_health = (
                success.get("money_proof_health")
                if isinstance(success.get("money_proof_health"), dict)
                else {}
            )
            if not money_proof_health:
                money_proof_health = _money_proof_health(
                    money_proof_mandate=money_proof_mandate,
                    autonomous_money_mandate=autonomous_money_mandate,
                    launch_readiness=launch_readiness,
                    active_sends=active_sends,
                    active_remaining=active_remaining,
                    payments=payments,
                    replies=replies,
                    checkout_clicks=checkout_clicks,
                )
            checks["relay_success"]["money_proof_health"] = money_proof_health
            checks["money_system"] = {
                "state": money_state,
                "gross_usd": money.get("gross_usd", 0),
                "payments": payments,
                "operator_mode": operator_mode,
                "launch_readiness": launch_readiness,
                "revenue_objective": revenue_objective,
                "money_decision_contract": money_decision_contract,
                "reply_autonomy_contract": reply_autonomy_contract,
                "conversion_ladder": conversion_ladder,
                "success_governor": success_governor,
                "owner_absence_contract": owner_absence_contract,
                "autonomous_money_mandate": autonomous_money_mandate,
                "money_proof_mandate": money_proof_mandate,
                "money_proof_health": money_proof_health,
                "revenue_ladder": revenue_ladder,
                "close_path": {
                    "replies": replies,
                    "auto_replies": auto_replies,
                    "unhandled_replies": unhandled_replies,
                    "auto_closed_replies": auto_closed_replies,
                    "reply_autoclose_mode": reply_autonomy_contract.get("reply_autoclose_mode"),
                    "checkout_clicks": checkout_clicks,
                    "reply_to_payment_gap": unhandled_replies,
                    "auto_reply_to_payment_gap": max(auto_replies - payments, 0),
                    "checkout_to_payment_gap": max(checkout_clicks - payments, 0),
                },
                "active_experiment_progress": f"{active_sends}/{active_target}" if active_target else "",
                "active_experiment_sends_remaining": active_remaining,
                "active_experiment_signal_window": performance.get("active_experiment_signal_window") or {},
                "active_reply_observation": active_reply_observation,
                "expected_next_window_sends": launch_readiness.get("expected_next_window_sends"),
                "readiness_state": launch_readiness.get("readiness_state"),
                "readiness_reason": launch_readiness.get("readiness_reason"),
                "expected_progress_after_next_window": launch_readiness.get("expected_progress_after_next_window"),
                "next_window_success_criterion": launch_readiness.get("next_window_success_criterion"),
                "next_window_audit_at": launch_readiness.get("next_window_audit_at"),
                "window_execution_state": launch_readiness.get("window_execution_state"),
                "window_execution_failure_condition": launch_readiness.get("window_execution_failure_condition"),
                "window_execution_contract": launch_readiness.get("window_execution_contract"),
                "active_experiment_windows_to_complete_at_current_cap": sample_windows_to_complete,
                "active_experiment_queued_sample_covers_remaining": queued_sample_covers_remaining,
                "active_experiment_decision_state": experiment_decision_state,
                "active_experiment_decision_next": experiment_decision_next,
                "queued_direct_leads": outreach.get("direct_due_count"),
                "cap_remaining": cap_remaining,
                "loop_status": loop_status,
                "next_autonomous_window": outreach.get("send_window_next_open_local"),
                "next_action": success.get("next_action"),
            }
        except Exception as exc:
            checks["relay_performance"] = {
                "status": "error",
                "summary": str(exc),
            }
        checks["verdict"] = _ready_label(checks)
        if isinstance(checks.get("money_system"), dict):
            checks["money_system"]["verdict"] = checks["verdict"]
        return checks
    finally:
        db.close()
