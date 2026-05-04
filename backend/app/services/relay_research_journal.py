from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent


RELAY_RESEARCH_JOURNAL_EVENT = "relay_research_journal_entry"
RELAY_RESEARCH_JOURNAL_VERSION = "relay-research-journal-v1"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _status_label(value: Any) -> str:
    if value is None:
        return "none"
    if not isinstance(value, dict):
        return str(value)[:120]
    status = str(value.get("status") or "").strip()
    reason = str(value.get("reason") or value.get("summary") or "").strip()
    if status and reason:
        return f"{status}:{reason}"[:160]
    return (status or reason or "snapshot")[:160]


def _primary_success_control(result: dict[str, Any]) -> dict[str, Any]:
    after = _as_dict(result.get("success_control_after_outreach"))
    before = _as_dict(result.get("success_control"))
    if after and after.get("status") != "skipped" and (after.get("after") or after.get("money_proof_mandate")):
        return after
    return before if before else after


def _snapshot_from_success(success: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(success.get("after")) or _as_dict(success.get("before"))


def _money_from_success(success: dict[str, Any]) -> dict[str, Any]:
    snapshot = _snapshot_from_success(success)
    money = _as_dict(snapshot.get("money"))
    mandate = _as_dict(success.get("money_proof_mandate")) or _as_dict(success.get("before_money_proof_mandate"))
    score = _as_dict(mandate.get("score"))
    gross_usd = _safe_float(money.get("gross_usd") if money else score.get("gross_usd"))
    payments = _safe_int(money.get("payments") if money else score.get("payments"))
    return {
        "gross_usd": round(gross_usd, 2),
        "payments": payments,
        "money_truth": "monetized" if gross_usd > 0 or payments > 0 else "not_monetized_yet",
    }


def _compact_decision(success: dict[str, Any]) -> dict[str, Any]:
    mandate = _as_dict(success.get("money_proof_mandate")) or _as_dict(success.get("before_money_proof_mandate"))
    health = _as_dict(success.get("money_proof_health")) or _as_dict(success.get("before_money_proof_health"))
    return {
        "bottleneck": success.get("bottleneck"),
        "next_action": success.get("next_action"),
        "after_bottleneck": success.get("after_bottleneck"),
        "after_next_action": success.get("after_next_action"),
        "bottleneck_changed": bool(success.get("bottleneck_changed")),
        "proof_state": mandate.get("state"),
        "primary_action": mandate.get("primary_action"),
        "owner_policy": mandate.get("owner_policy"),
        "success_condition": mandate.get("success_condition"),
        "proof_deadline": mandate.get("proof_deadline"),
        "proof_health_state": health.get("state"),
        "proof_health_reason": health.get("reason"),
        "autonomous_recovery_action": health.get("autonomous_recovery_action"),
        "owner_interrupt": bool(health.get("owner_interrupt")),
        "do_not_judge_demand": bool(health.get("do_not_judge_demand")),
    }


def _compact_snapshot(success: dict[str, Any]) -> dict[str, Any]:
    snapshot = _snapshot_from_success(success)
    outreach = _as_dict(snapshot.get("outreach"))
    intent = _as_dict(snapshot.get("intent"))
    conversion = _as_dict(snapshot.get("conversion"))
    active_reply_observation = _as_dict(snapshot.get("active_reply_observation"))
    return {
        "sends": _safe_int(outreach.get("sends")),
        "replies": _safe_int(outreach.get("replies")),
        "auto_replies": _safe_int(outreach.get("auto_replies")),
        "unhandled_replies": _safe_int(outreach.get("unhandled_replies")),
        "checkout_clicks": _safe_int(intent.get("checkout_clicks")),
        "messy_notes_leads": _safe_int(intent.get("messy_notes_leads")),
        "sample_request_leads": _safe_int(intent.get("sample_request_leads")),
        "checkout_intent_leads": _safe_int(intent.get("checkout_intent_leads")),
        "conversion": {
            "messy_notes_due": _safe_int(conversion.get("messy_notes_due")),
            "sample_request_due": _safe_int(conversion.get("sample_request_due")),
            "checkout_intent_due": _safe_int(conversion.get("checkout_intent_due")),
        },
        "active_reply_observation": {
            "pending": bool(active_reply_observation.get("pending")),
            "complete": bool(active_reply_observation.get("complete")),
            "observe_until": active_reply_observation.get("observe_until") or "",
            "reason": active_reply_observation.get("reason") or "",
        },
    }


def build_success_control_journal_entry(result: dict[str, Any]) -> dict[str, Any]:
    money = _money_from_success(result)
    decision = _compact_decision(result)
    evidence = _compact_snapshot(result)
    actions = _as_dict(result.get("actions"))
    conversion_actions = _as_dict(result.get("conversion_actions"))
    action_failures = _as_dict(result.get("action_failures"))
    summary = (
        f"success_control money=${money['gross_usd']:.2f} payments={money['payments']} "
        f"bottleneck={decision.get('after_bottleneck') or decision.get('bottleneck')} "
        f"proof={decision.get('proof_state')} health={decision.get('proof_health_state')} "
        f"conversion_sent={_safe_int(conversion_actions.get('sent_count'))} "
        f"failures={len(action_failures)}"
    )
    return {
        "version": RELAY_RESEARCH_JOURNAL_VERSION,
        "source": "success_control",
        "created_at": datetime.utcnow().isoformat(),
        "summary": summary,
        "hypothesis": {
            "success_condition": decision.get("success_condition"),
            "primary_action": decision.get("primary_action"),
            "proof_deadline": decision.get("proof_deadline"),
        },
        "decision": decision,
        "evidence": {
            "money": money,
            **evidence,
        },
        "actions": {
            name: _status_label(action)
            for name, action in actions.items()
        },
        "conversion_actions": conversion_actions,
        "failures": action_failures,
        "guardrails": {
            "owner_policy": decision.get("owner_policy"),
            "owner_interrupt": decision.get("owner_interrupt"),
            "do_not_judge_demand": decision.get("do_not_judge_demand"),
            "one_variable_rotation": True,
            "no_test_leads": True,
        },
    }


def build_money_loop_journal_entry(result: dict[str, Any]) -> dict[str, Any]:
    success = _primary_success_control(result)
    money = _money_from_success(success)
    decision = _compact_decision(success)
    status_after = _as_dict(result.get("status_after"))
    active_progress = result.get("active_sample_progress_after") or (
        f"{_safe_int(status_after.get('active_experiment_sends'))}/"
        f"{_safe_int(status_after.get('active_experiment_sample_target'))}"
        if status_after.get("active_experiment_sample_target") is not None
        else ""
    )
    summary = (
        f"money_loop money=${money['gross_usd']:.2f} payments={money['payments']} "
        f"sent={_safe_int(result.get('sent_this_tick'))} "
        f"active_delta={_safe_int(result.get('active_sample_delta_this_tick'))}/"
        f"{_safe_int(result.get('active_sample_expected_delta_this_tick'))} "
        f"active={active_progress} proof={result.get('active_sample_tick_proof_state')} "
        f"next={decision.get('primary_action') or decision.get('after_next_action') or decision.get('next_action')}"
    )
    return {
        "version": RELAY_RESEARCH_JOURNAL_VERSION,
        "source": "money_loop",
        "created_at": datetime.utcnow().isoformat(),
        "summary": summary,
        "hypothesis": {
            "success_condition": decision.get("success_condition"),
            "primary_action": decision.get("primary_action"),
            "proof_deadline": decision.get("proof_deadline"),
        },
        "decision": decision,
        "evidence": {
            "money": money,
            "sent_this_tick": _safe_int(result.get("sent_this_tick")),
            "active_sample_sends_before": _safe_int(result.get("active_sample_sends_before")),
            "active_sample_sends_after": _safe_int(result.get("active_sample_sends_after")),
            "active_sample_delta_this_tick": _safe_int(result.get("active_sample_delta_this_tick")),
            "active_sample_expected_delta_this_tick": _safe_int(result.get("active_sample_expected_delta_this_tick")),
            "active_sample_tick_proof_state": result.get("active_sample_tick_proof_state"),
            "active_sample_progress_after": active_progress,
            "sendable_due_before": _safe_int(result.get("sendable_due_before")),
            "sendable_due_mode": result.get("sendable_due_mode"),
            "direct_due_before": _safe_int(result.get("direct_due_before")),
            "active_experiment_new_due_before": _safe_int(result.get("active_experiment_new_due_before")),
            "cap_remaining_before": _safe_int(result.get("cap_remaining_before")),
            "status_after": {
                "active_experiment_variant": status_after.get("active_experiment_variant"),
                "active_experiment_sends": status_after.get("active_experiment_sends"),
                "active_experiment_sample_target": status_after.get("active_experiment_sample_target"),
                "active_experiment_new_due_count": status_after.get("active_experiment_new_due_count"),
                "direct_due_count": status_after.get("direct_due_count"),
                "cap_remaining": status_after.get("cap_remaining"),
                "send_window_reason": status_after.get("send_window_reason"),
                "send_window_next_open_local": status_after.get("send_window_next_open_local"),
                "next_money_move": status_after.get("next_money_move"),
            },
        },
        "actions": {
            "refill": _status_label(result.get("refill_result")),
            "outreach": _status_label(result.get("outreach_result")),
            "post_refill_outreach": _status_label(result.get("post_refill_outreach_result")),
            "success_control": _status_label(result.get("success_control")),
            "success_control_after_outreach": _status_label(result.get("success_control_after_outreach")),
        },
        "failures": {
            "success_control": _as_dict(success.get("action_failures")),
        },
        "guardrails": {
            "send_live": bool(result.get("send_live")),
            "force_refill": bool(result.get("force_refill")),
            "send_window_open_before": bool(result.get("send_window_open_before")),
            "owner_policy": decision.get("owner_policy"),
            "owner_interrupt": decision.get("owner_interrupt"),
            "do_not_judge_demand": decision.get("do_not_judge_demand"),
            "one_variable_rotation": True,
            "no_test_leads": True,
        },
    }


def _write_journal_entry(entry: dict[str, Any]) -> None:
    with SessionLocal() as session:
        session.add(
            AcquisitionEvent(
                event_type=RELAY_RESEARCH_JOURNAL_EVENT,
                prospect_external_id=f"relay-research:{entry.get('source') or 'unknown'}",
                summary=str(entry.get("summary") or "")[:500],
                payload_json=json.dumps(entry, ensure_ascii=False, sort_keys=True),
            )
        )
        session.commit()


def log_success_control_journal(result: dict[str, Any]) -> None:
    try:
        _write_journal_entry(build_success_control_journal_entry(result))
    except Exception:
        return


def log_money_loop_journal(result: dict[str, Any]) -> None:
    try:
        _write_journal_entry(build_money_loop_journal_entry(result))
    except Exception:
        return
