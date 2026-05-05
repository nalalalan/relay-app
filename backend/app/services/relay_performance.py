from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect


PERFORMANCE_REVIEW_EVENT = "relay_performance_weekly_review"
EXPERIMENT_PLAN_EVENT = "relay_experiment_plan"
RESEARCH_INPUT_EVENT = "relay_optimizer_research_input"

DEFAULT_EXPERIMENT_VARIANT = "control_sample_ask"
CURRENT_VERSION = "relay_optimizer_v2"
ESCALATED_MONEY_EXPERIMENTS = {
    "hard_paid_test_direct",
    "stalled_opportunity_direct",
    "revenue_leak_direct",
}

GENERIC_INBOX_LOCAL_PARTS = {
    "admin",
    "contact",
    "hello",
    "hi",
    "info",
    "inquiries",
    "jobs",
    "mail",
    "marketing",
    "media",
    "news",
    "office",
    "partnerships",
    "press",
    "pr",
    "sales",
    "support",
    "team",
}

EXPERIMENTS: dict[str, dict[str, Any]] = {
    "control_sample_ask": {
        "label": "Control: sample ask",
        "hypothesis": "Plain after-call cleanup language is enough if the lead is a real fit.",
        "query_rotation": [
            "paid media agency founder",
            "google ads agency owner",
            "performance marketing agency founder",
        ],
        "change": "Keep the current direct sample ask.",
    },
    "sample_first_plain": {
        "label": "Sample first",
        "hypothesis": "Leading with the sample makes the offer concrete before asking for interest.",
        "query_rotation": [
            "b2b paid media agency founder",
            "google ads agency managing partner",
            "meta ads agency owner",
        ],
        "change": "Put the sample earlier and keep the ask low-friction.",
    },
    "pain_owner_direct": {
        "label": "Owner of cleanup pain",
        "hypothesis": "Asking who owns after-call cleanup will expose whether the pain exists.",
        "query_rotation": [
            "agency owner client follow up",
            "paid media agency operations director",
            "founder led marketing agency",
        ],
        "change": "Make the first email about ownership of the after-call cleanup job.",
    },
    "paid_test_explicit": {
        "label": "Explicit paid test",
        "hypothesis": "Being clear about the $40 test filters for buyers and reduces vague curiosity.",
        "query_rotation": [
            "growth marketing agency founder",
            "ppc agency owner",
            "b2b lead generation agency owner",
        ],
        "change": "Mention the $40 test earlier after enough reply signal exists.",
    },
    "hard_paid_test_direct": {
        "label": "Hard paid test",
        "hypothesis": "If free sample language gets no reply signal, direct paid-outcome language may reveal actual buyers.",
        "sample_target": 10,
        "query_rotation": [
            "agency owner client follow up",
            "b2b agency operations founder",
            "sales operations agency owner",
        ],
        "change": "Lead with the concrete $40 paid test and the after-call follow-up outcome instead of a free sample ask.",
    },
    "stalled_opportunity_direct": {
        "label": "Stalled opportunity",
        "hypothesis": "A stalled-opportunity framing may reach buyers who ignore generic after-call cleanup language.",
        "sample_target": 10,
        "query_rotation": [
            "agency owner proposal follow up",
            "marketing agency sales operations founder",
            "b2b agency founder client follow up",
        ],
        "change": "Frame the $40 test as finishing one stalled sales follow-up.",
    },
    "revenue_leak_direct": {
        "label": "Revenue leak angle",
        "hypothesis": "Positioning delayed follow-up as a revenue leak may reach buyers who ignore generic cleanup language.",
        "sample_target": 10,
        "query_rotation": [
            "agency sales operations founder",
            "marketing agency client success operations",
            "b2b agency account director",
        ],
        "change": "Frame the $40 test around preventing delayed client follow-up from leaking revenue.",
    },
}

DEFAULT_RESEARCH_SOURCES = [
    "https://support.google.com/a/answer/14229414",
    "https://support.google.com/mail/answer/81126",
    "https://senders.yahooinc.com/best-practices/",
    "https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business",
]


def _session() -> Session:
    return SessionLocal()


def _now() -> datetime:
    return datetime.utcnow()


def _week_start(value: datetime | None = None) -> datetime:
    value = value or _now()
    start = value - timedelta(days=value.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _experiment_start(plan: dict[str, Any], default: datetime) -> tuple[datetime, str]:
    for key in ("created_at", "logged_at", "week_start", "week_start_date"):
        parsed = _parse_datetime(plan.get(key))
        if parsed is not None:
            return parsed, key
    return default, "rolling_7_day_fallback"


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _event_payload(row: AcquisitionEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _safe_json(row.payload_json)
    if payload:
        payload.setdefault("logged_at", row.created_at.isoformat() if row.created_at else "")
        payload.setdefault("event_id", row.id)
    return payload or None


def _latest_event(session: Session, event_type: str) -> AcquisitionEvent | None:
    return session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == event_type)
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _event_count(
    session: Session,
    event_type: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    like: bool = False,
) -> int:
    stmt = select(func.count(AcquisitionEvent.id))
    if like:
        stmt = stmt.where(AcquisitionEvent.event_type.like(event_type))
    else:
        stmt = stmt.where(AcquisitionEvent.event_type == event_type)
    if start is not None:
        stmt = stmt.where(AcquisitionEvent.created_at >= start)
    if end is not None:
        stmt = stmt.where(AcquisitionEvent.created_at < end)
    return int(session.execute(stmt).scalar() or 0)


def _stripe_metrics(session: Session, *, start: datetime, end: datetime) -> dict[str, Any]:
    events = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .where(AcquisitionEvent.created_at >= start)
        .where(AcquisitionEvent.created_at < end)
        .order_by(AcquisitionEvent.created_at.desc())
    ).scalars().all()

    gross_cents = 0
    payments = 0
    for event in events:
        payload = _safe_json(event.payload_json)
        email = str(
            payload.get("customer_details", {}).get("email")
            or payload.get("customer_email")
            or payload.get("email")
            or ""
        ).strip().lower()
        if email == "pham.alann@gmail.com":
            continue
        gross_cents += int(payload.get("amount_total") or 0)
        payments += 1

    return {
        "payments": payments,
        "gross_usd": round(gross_cents / 100.0, 2),
        "gross_cents": gross_cents,
    }


def _is_generic_inbox(email_address: str) -> bool:
    local = (email_address or "").split("@", 1)[0].strip().lower()
    if not local:
        return True
    local_base = local.replace(".", "").replace("-", "").replace("_", "")
    if local in GENERIC_INBOX_LOCAL_PARTS or local_base in GENERIC_INBOX_LOCAL_PARTS:
        return True
    return local.startswith(("info", "hello", "contact", "admin", "support", "sales", "media", "press"))


def _prospect_health(session: Session) -> dict[str, Any]:
    active_statuses = ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
    prospects = session.execute(
        select(AcquisitionProspect)
        .where(AcquisitionProspect.contact_email != "")
        .where(AcquisitionProspect.status.in_(active_statuses))
    ).scalars().all()

    direct = 0
    generic = 0
    fit_scores: list[int] = []
    statuses: dict[str, int] = {}
    for prospect in prospects:
        if _is_generic_inbox(prospect.contact_email):
            generic += 1
        else:
            direct += 1
        fit_scores.append(int(prospect.fit_score or 0))
        statuses[prospect.status] = statuses.get(prospect.status, 0) + 1

    return {
        "active_prospects": len(prospects),
        "direct_inbox_count": direct,
        "generic_inbox_count": generic,
        "avg_fit_score": round(sum(fit_scores) / len(fit_scores), 1) if fit_scores else 0,
        "status_counts": statuses,
    }


def _metrics_for_window(session: Session, *, start: datetime, end: datetime) -> dict[str, Any]:
    sends = _event_count(session, "custom_outreach_sent_step_%", start=start, end=end, like=True)
    replies = (
        _event_count(session, "custom_outreach_reply_seen", start=start, end=end)
        + _event_count(session, "smartlead_reply", start=start, end=end)
    )
    failures = _event_count(session, "custom_outreach_send_failed", start=start, end=end)
    auto_replies = _event_count(session, "custom_outreach_auto_reply_sent", start=start, end=end)
    stripe = _stripe_metrics(session, start=start, end=end)

    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sends": sends,
        "replies": replies,
        "reply_rate": round(replies / sends, 4) if sends else 0,
        "send_failures": failures,
        "auto_replies": auto_replies,
        "payments": stripe["payments"],
        "gross_usd": stripe["gross_usd"],
    }


def _event_variant(event: AcquisitionEvent) -> str:
    payload = _safe_json(event.payload_json)
    variant = str(payload.get("experiment_variant") or "").strip()
    if variant in EXPERIMENTS:
        return variant
    return DEFAULT_EXPERIMENT_VARIANT


def _event_step_number(event: AcquisitionEvent) -> int:
    payload = _safe_json(event.payload_json)
    try:
        return int(payload.get("step_number") or 0)
    except Exception:
        pass
    raw_type = str(getattr(event, "event_type", "") or "")
    try:
        return int(raw_type.rsplit("_", 1)[-1])
    except Exception:
        return 0


def _event_is_first_touch_sample(event: AcquisitionEvent) -> bool:
    payload = _safe_json(event.payload_json)
    marker = payload.get("active_experiment_first_touch")
    if isinstance(marker, bool):
        return marker
    if marker is not None:
        return str(marker).strip().lower() in {"1", "true", "yes", "y"}
    return _event_step_number(event) == 1


def _variant_metrics_for_window(
    session: Session,
    *,
    variant: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    variant = variant if variant in EXPERIMENTS else DEFAULT_EXPERIMENT_VARIANT
    sent_events = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
        .where(AcquisitionEvent.created_at >= start)
        .where(AcquisitionEvent.created_at < end)
        .order_by(AcquisitionEvent.created_at.asc())
    ).scalars().all()
    variant_sends = [event for event in sent_events if _event_variant(event) == variant]
    matching_sends = [event for event in variant_sends if _event_is_first_touch_sample(event)]
    prospect_ids = {
        str(event.prospect_external_id or "").strip()
        for event in matching_sends
        if str(event.prospect_external_id or "").strip()
    }

    replies = 0
    payments = 0
    if prospect_ids:
        replies = int(
            session.execute(
                select(func.count(AcquisitionEvent.id))
                .where(AcquisitionEvent.prospect_external_id.in_(prospect_ids))
                .where(AcquisitionEvent.event_type.in_(["custom_outreach_reply_seen", "smartlead_reply"]))
                .where(AcquisitionEvent.created_at >= start)
                .where(AcquisitionEvent.created_at < end)
            ).scalar()
            or 0
        )
        payments = int(
            session.execute(
                select(func.count(AcquisitionProspect.id))
                .where(AcquisitionProspect.external_id.in_(prospect_ids))
                .where(AcquisitionProspect.stripe_status == "paid")
            ).scalar()
            or 0
        )

    failed_events = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "custom_outreach_send_failed")
        .where(AcquisitionEvent.created_at >= start)
        .where(AcquisitionEvent.created_at < end)
    ).scalars().all()
    failures = sum(1 for event in failed_events if _event_variant(event) == variant)
    total_variant_sends = len(variant_sends)
    sends = len(matching_sends)
    first_sent = matching_sends[0].created_at.isoformat() if matching_sends else ""
    last_sent = matching_sends[-1].created_at.isoformat() if matching_sends else ""
    last_variant_sent = variant_sends[-1].created_at.isoformat() if variant_sends else ""

    return {
        "variant": variant,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sends": sends,
        "sample_sends": sends,
        "total_variant_sends": total_variant_sends,
        "prospect_count": len(prospect_ids),
        "replies": replies,
        "reply_rate": round(replies / sends, 4) if sends else 0,
        "payments": payments,
        "send_failures": failures,
        "first_sent_at": first_sent,
        "last_sent_at": last_sent,
        "last_variant_sent_at": last_variant_sent,
    }


def _research_urls() -> list[str]:
    raw = os.getenv("RELAY_OPTIMIZER_RESEARCH_URLS", "").strip()
    urls = [item.strip() for item in raw.split("|") if item.strip()] if raw else []
    merged: list[str] = []
    for url in urls + DEFAULT_RESEARCH_SOURCES:
        if url and url not in merged:
            merged.append(url)
    return merged[:8]


def _html_title(text: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.I | re.S)
    if not match:
        return ""
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return re.sub(r"<[^>]+>", "", title)[:180]


def fetch_online_research_inputs() -> list[dict[str, Any]]:
    inputs: list[dict[str, Any]] = []
    headers = {"User-Agent": "relay-optimizer/1.0"}
    for url in _research_urls():
        item: dict[str, Any] = {
            "url": url,
            "fetched_at": _now().isoformat(),
        }
        try:
            response = httpx.get(url, headers=headers, follow_redirects=True, timeout=6.0)
            item["status_code"] = response.status_code
            item["title"] = _html_title(response.text)
            item["ok"] = 200 <= response.status_code < 400
        except Exception as exc:
            item["ok"] = False
            item["error"] = f"{type(exc).__name__}: {exc}"
        inputs.append(item)
    return inputs


def _knowledge_takeaways() -> list[dict[str, str]]:
    return [
        {
            "source": "Google sender guidelines",
            "takeaway": "Keep spam complaints very low, authenticate mail, and prefer easy unsubscribe for promotional mail.",
        },
        {
            "source": "Yahoo Sender Hub",
            "takeaway": "Relevant mail, low complaint rate, SPF/DKIM/DMARC, and easy opt-out protect deliverability.",
        },
        {
            "source": "FTC CAN-SPAM guide",
            "takeaway": "Headers and subject lines must be honest; commercial mail needs a clear opt-out path.",
        },
        {
            "source": "Relay internal data",
            "takeaway": "Change one thing per week so replies and payments can be attributed to a specific test.",
        },
    ]


def _latest_plan_payload(session: Session) -> dict[str, Any] | None:
    return _event_payload(_latest_event(session, EXPERIMENT_PLAN_EVENT))


def _current_week_plan_payload(session: Session, week_start: datetime | None = None) -> dict[str, Any] | None:
    week_start = week_start or _week_start()
    rows = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == EXPERIMENT_PLAN_EVENT)
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(12)
    ).scalars().all()
    week_key = week_start.date().isoformat()
    for row in rows:
        payload = _event_payload(row)
        if payload and payload.get("week_start_date") == week_key:
            return payload
    return None


def _int_from(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _min_experiment_sample() -> int:
    return max(1, _int_from(os.getenv("RELAY_EXPERIMENT_MIN_SAMPLE", "20"), 20))


def experiment_sample_target(plan_or_variant: dict[str, Any] | str | None = None) -> int:
    default = _min_experiment_sample()
    variant = ""
    if isinstance(plan_or_variant, dict):
        direct_target = _int_from(plan_or_variant.get("sample_target"), 0)
        if direct_target > 0:
            return direct_target
        variant = str(plan_or_variant.get("experiment_variant") or "").strip()
    elif plan_or_variant is not None:
        variant = str(plan_or_variant or "").strip()

    experiment = EXPERIMENTS.get(variant)
    if isinstance(experiment, dict):
        experiment_target = _int_from(experiment.get("sample_target"), 0)
        if experiment_target > 0:
            return experiment_target
    return default


def _zero_signal_rotation_threshold() -> int:
    return max(1, _int_from(os.getenv("RELAY_ZERO_SIGNAL_ROTATION_ESCALATION_COUNT", "2"), 2))


def _reply_observation_hours() -> int:
    return max(1, _int_from(os.getenv("RELAY_ACTIVE_REPLY_OBSERVATION_HOURS", "24"), 24))


def _reply_observation_pending(metrics: dict[str, Any] | None, min_sample: int, now: datetime) -> bool:
    if not isinstance(metrics, dict):
        return False
    sends = _int_from(metrics.get("sends"))
    replies = _int_from(metrics.get("replies"))
    payments = _int_from(metrics.get("payments"))
    if sends < min_sample or replies > 0 or payments > 0:
        return False
    last_sent_at = _parse_datetime(metrics.get("last_sent_at"))
    if last_sent_at is None:
        return False
    return now < last_sent_at + timedelta(hours=_reply_observation_hours())


def _zero_signal_plan(
    payload: dict[str, Any],
    min_sample: int,
    *,
    live_metrics: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    decision_reasons = [
        str(reason)
        for reason in payload.get("decision_reasons", [])
        if str(reason).strip()
    ]
    if live_metrics is not None:
        sends = _int_from(live_metrics.get("sends"))
        replies = _int_from(live_metrics.get("replies"))
        payments = _int_from(live_metrics.get("payments"))
        if sends < min_sample:
            return False, "pending_sample"
        if replies > 0 or payments > 0:
            return False, "signal_found"
        if _reply_observation_pending(live_metrics, min_sample, _now()):
            return False, "reply_observation_pending"
        return True, "zero_signal"

    reason_text = " ".join(decision_reasons).lower()
    windows = [
        payload.get("rolling_7_day_metrics"),
        payload.get("prior_week_metrics"),
        payload.get("current_week_metrics"),
    ]
    for metrics in windows:
        if not isinstance(metrics, dict):
            continue
        if _int_from(metrics.get("replies")) > 0 or _int_from(metrics.get("payments")) > 0:
            return False, "signal_found"
    for metrics in windows:
        if not isinstance(metrics, dict):
            continue
        if (
            _int_from(metrics.get("sends")) >= min_sample
            and _int_from(metrics.get("replies")) <= 0
            and _int_from(metrics.get("payments")) <= 0
        ):
            return True, "zero_signal"
    if "no reply signal" in reason_text and "measurable sends" in reason_text:
        return True, "zero_signal"
    return False, "unknown"


def _plan_variant_sample_metrics(
    session: Session,
    *,
    plan: dict[str, Any],
    plan_created_at: datetime | None,
    now: datetime,
) -> dict[str, Any] | None:
    variant = str(plan.get("experiment_variant") or "").strip()
    if not variant:
        return None
    start = plan_created_at or _parse_datetime(plan.get("logged_at")) or _parse_datetime(plan.get("created_at"))
    if start is None:
        return None
    return _variant_metrics_for_window(session, variant=variant, start=start, end=now)


def _latest_plan_sample_pending(session: Session, latest_plan: dict[str, Any] | None, *, now: datetime) -> bool:
    if not latest_plan:
        return False
    min_sample = experiment_sample_target(latest_plan)
    metrics = _plan_variant_sample_metrics(
        session,
        plan=latest_plan,
        plan_created_at=_parse_datetime(latest_plan.get("logged_at")),
        now=now,
    )
    return metrics is not None and (
        _int_from(metrics.get("sends")) < min_sample
        or _reply_observation_pending(metrics, min_sample, now)
    )


def _zero_signal_rotation_count(session: Session) -> int:
    threshold = _zero_signal_rotation_threshold()
    now = _now()
    rows = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == EXPERIMENT_PLAN_EVENT)
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(max(threshold + 4, 6))
    ).scalars().all()

    count = 0
    for row in rows:
        payload = _safe_json(row.payload_json)
        min_sample = experiment_sample_target(payload)
        live_metrics = _plan_variant_sample_metrics(
            session,
            plan=payload,
            plan_created_at=row.created_at,
            now=now,
        )
        is_zero_signal, state = _zero_signal_plan(payload, min_sample, live_metrics=live_metrics)
        if not is_zero_signal:
            if state in {"pending_sample", "reply_observation_pending"} and count == 0:
                continue
            break
        count += 1
    return count


def _variant_sequence() -> list[str]:
    raw = os.getenv("RELAY_EXPERIMENT_SEQUENCE", "").strip()
    variants = [item.strip() for item in raw.split("|") if item.strip()] if raw else []
    valid = [item for item in variants if item in EXPERIMENTS]
    return valid or list(EXPERIMENTS.keys())


def _choose_variant(
    *,
    current_week: dict[str, Any],
    prior_week: dict[str, Any],
    rolling_window: dict[str, Any] | None = None,
    latest_plan: dict[str, Any] | None,
    zero_signal_rotation_count: int = 0,
    latest_plan_sample_pending: bool = False,
) -> tuple[str, list[str]]:
    min_sample = experiment_sample_target(latest_plan) if latest_plan else _min_experiment_sample()
    evidence_candidates = [
        ("prior week", prior_week),
        ("rolling 7-day window", rolling_window or {}),
        ("current week", current_week),
    ]
    evidence_name, evidence = next(
        (
            (name, metrics)
            for name, metrics in evidence_candidates
            if int(metrics.get("sends", 0)) >= min_sample
        ),
        ("current week", current_week),
    )
    sends = int(evidence.get("sends", 0))
    replies = int(evidence.get("replies", 0))
    payments = int(evidence.get("payments", 0))
    failures = int(evidence.get("send_failures", 0))
    sequence = _variant_sequence()
    previous_variant = str((latest_plan or {}).get("experiment_variant") or DEFAULT_EXPERIMENT_VARIANT)
    reasons: list[str] = []

    if failures > 0 and failures >= sends:
        reasons.append("Send failures dominated the last window; keep copy stable and fix infrastructure first.")
        return previous_variant if previous_variant in EXPERIMENTS else DEFAULT_EXPERIMENT_VARIANT, reasons

    if sends < min_sample:
        reasons.append(f"Sample is still small; avoid random thrashing until at least {min_sample} sends land.")
        return previous_variant if previous_variant in EXPERIMENTS else DEFAULT_EXPERIMENT_VARIANT, reasons

    if payments > 0:
        reasons.append(f"Payment signal exists in the {evidence_name}; keep the current lane and collect more evidence.")
        return previous_variant if previous_variant in EXPERIMENTS else DEFAULT_EXPERIMENT_VARIANT, reasons

    if replies > 0:
        reasons.append(f"Reply signal exists in the {evidence_name} without payments; make the paid test clearer.")
        return "paid_test_explicit", reasons

    if latest_plan_sample_pending and previous_variant in EXPERIMENTS:
        reasons.append("Latest active experiment still needs its own full evaluation window; keep it stable before rotating.")
        return previous_variant, reasons

    reasons.append(f"No reply signal after measurable sends in the {evidence_name}; rotate one controlled copy/targeting variable.")
    if latest_plan_sample_pending and previous_variant in ESCALATED_MONEY_EXPERIMENTS:
        reasons.append("The escalated paid-offer lane still needs its own sample; do not rotate before evidence.")
        return previous_variant, reasons
    if zero_signal_rotation_count + 1 >= _zero_signal_rotation_threshold():
        if previous_variant == "stalled_opportunity_direct":
            reasons.append("The stalled-opportunity lane completed without signal; test the revenue-leak angle before returning to softer sample copy.")
            return "revenue_leak_direct", reasons
        if previous_variant == "revenue_leak_direct":
            reasons.append("The revenue-leak lane completed without signal; return to explicit paid-test copy before trying softer sample copy.")
            return "paid_test_explicit", reasons
        if previous_variant != "hard_paid_test_direct":
            reasons.append("Repeated no-reply/no-payment rotations reached the escalation threshold; test the harder paid-offer lane.")
            return "hard_paid_test_direct", reasons
        reasons.append("The hard paid-offer lane completed without signal; test a stalled-opportunity offer before returning to softer sample copy.")
        return "stalled_opportunity_direct", reasons
    if previous_variant not in sequence:
        return sequence[0], reasons
    return sequence[(sequence.index(previous_variant) + 1) % len(sequence)], reasons


def _daily_cap_recommendation(metrics: dict[str, Any]) -> int:
    sends = int(metrics.get("sends", 0))
    replies = int(metrics.get("replies", 0))
    failures = int(metrics.get("send_failures", 0))
    current_cap = int(os.getenv("BUYER_ACQ_DAILY_SEND_CAP", os.getenv("ACQ_DAILY_SEND_CAP", "20")) or "20")
    if failures > 0:
        return min(current_cap, 10)
    if sends >= 30 and replies == 0:
        return min(current_cap, 10)
    if replies > 0:
        return min(max(current_cap, 10), 20)
    return min(current_cap, 10)


def _plan_payload(
    *,
    week_start: datetime,
    variant: str,
    current_week: dict[str, Any],
    prior_week: dict[str, Any],
    rolling_window: dict[str, Any],
    latest_plan: dict[str, Any] | None,
    research_inputs: list[dict[str, Any]],
    reasons: list[str],
    zero_signal_rotation_count: int,
    zero_signal_rotation_threshold: int,
) -> dict[str, Any]:
    experiment = EXPERIMENTS.get(variant, EXPERIMENTS[DEFAULT_EXPERIMENT_VARIANT])
    zero_signal_effective_count = zero_signal_rotation_count + (1 if variant in ESCALATED_MONEY_EXPERIMENTS else 0)
    evidence_for_cap = (
        prior_week
        if int(prior_week.get("sends", 0))
        else rolling_window
        if int(rolling_window.get("sends", 0))
        else current_week
    )
    return {
        "version": CURRENT_VERSION,
        "week_start": week_start.isoformat(),
        "week_start_date": week_start.date().isoformat(),
        "experiment_variant": variant,
        "experiment_label": experiment["label"],
        "hypothesis": experiment["hypothesis"],
        "change": experiment["change"],
        "query_rotation": experiment["query_rotation"],
        "sample_target": experiment_sample_target(variant),
        "daily_cap_recommendation": _daily_cap_recommendation(evidence_for_cap),
        "decision_reasons": reasons,
        "current_week_metrics": current_week,
        "prior_week_metrics": prior_week,
        "rolling_7_day_metrics": rolling_window,
        "previous_experiment_variant": (latest_plan or {}).get("experiment_variant"),
        "zero_signal_rotation_count": zero_signal_rotation_count,
        "zero_signal_current_evidence_count": zero_signal_effective_count,
        "zero_signal_rotation_threshold": zero_signal_rotation_threshold,
        "zero_signal_rotation_escalated": zero_signal_effective_count >= zero_signal_rotation_threshold,
        "knowledge_takeaways": _knowledge_takeaways(),
        "online_research_inputs": research_inputs,
        "guardrails": [
            "One major change per week.",
            "Do not increase volume when reply signal is zero.",
            "Prefer direct business contacts over generic inboxes.",
            "Keep subjects honest and keep opt-out handling obvious.",
        ],
        "created_at": _now().isoformat(),
    }


def run_weekly_performance_review(*, force: bool = False, fetch_research: bool = True) -> dict[str, Any]:
    now = _now()
    week_start = _week_start(now)
    current_start = week_start
    current_end = now
    prior_start = week_start - timedelta(days=7)
    prior_end = week_start
    rolling_start = now - timedelta(days=7)

    with _session() as session:
        existing = _current_week_plan_payload(session, week_start)
        if existing and not force:
            return {
                "status": "ok",
                "summary": "weekly plan already exists",
                "plan": existing,
                "created": False,
            }

        current_week = _metrics_for_window(session, start=current_start, end=current_end)
        prior_week = _metrics_for_window(session, start=prior_start, end=prior_end)
        rolling_window = _metrics_for_window(session, start=rolling_start, end=now)
        current_week["prospect_health"] = _prospect_health(session)
        latest_plan = _latest_plan_payload(session)
        zero_signal_rotations = _zero_signal_rotation_count(session)
        latest_plan_sample_pending = _latest_plan_sample_pending(session, latest_plan, now=now)

    research_inputs = fetch_online_research_inputs() if fetch_research else []
    zero_signal_threshold = _zero_signal_rotation_threshold()
    variant, reasons = _choose_variant(
        current_week=current_week,
        prior_week=prior_week,
        rolling_window=rolling_window,
        latest_plan=latest_plan,
        zero_signal_rotation_count=zero_signal_rotations,
        latest_plan_sample_pending=latest_plan_sample_pending,
    )
    plan = _plan_payload(
        week_start=week_start,
        variant=variant,
        current_week=current_week,
        prior_week=prior_week,
        rolling_window=rolling_window,
        latest_plan=latest_plan,
        research_inputs=research_inputs,
        reasons=reasons,
        zero_signal_rotation_count=zero_signal_rotations,
        zero_signal_rotation_threshold=zero_signal_threshold,
    )

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type=EXPERIMENT_PLAN_EVENT,
                prospect_external_id="relay-performance",
                summary=f"{plan['week_start_date']} {variant}: {plan['experiment_label']}",
                payload_json=json.dumps(plan, ensure_ascii=False),
            )
        )
        session.add(
            AcquisitionEvent(
                event_type=PERFORMANCE_REVIEW_EVENT,
                prospect_external_id="relay-performance",
                summary=(
                    f"weekly review sends={prior_week['sends']} "
                    f"replies={prior_week['replies']} gross=${prior_week['gross_usd']}"
                ),
                payload_json=json.dumps(
                    {
                        "version": CURRENT_VERSION,
                        "week_start": week_start.isoformat(),
                        "current_week_metrics": current_week,
                        "prior_week_metrics": prior_week,
                        "rolling_7_day_metrics": rolling_window,
                        "selected_plan": plan,
                    },
                    ensure_ascii=False,
                ),
            )
        )
        if research_inputs:
            session.add(
                AcquisitionEvent(
                    event_type=RESEARCH_INPUT_EVENT,
                    prospect_external_id="relay-performance",
                    summary=f"fetched {sum(1 for item in research_inputs if item.get('ok'))}/{len(research_inputs)} optimizer sources",
                    payload_json=json.dumps(
                        {
                            "version": CURRENT_VERSION,
                            "inputs": research_inputs,
                            "fetched_at": now.isoformat(),
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        session.commit()

    return {
        "status": "ok",
        "summary": "weekly plan created",
        "plan": plan,
        "created": True,
    }


def maybe_run_weekly_performance_review() -> dict[str, Any]:
    enabled = os.getenv("RELAY_WEEKLY_OPTIMIZER_ENABLED", "true").strip().lower() != "false"
    if not enabled:
        return {"status": "skipped", "summary": "weekly optimizer disabled"}
    return run_weekly_performance_review(force=False, fetch_research=True)


def active_relay_experiment() -> dict[str, Any]:
    week_start = _week_start()
    forced = os.getenv("RELAY_OUTREACH_VARIANT", "").strip()
    if forced in EXPERIMENTS:
        experiment = EXPERIMENTS[forced]
        return {
            "experiment_variant": forced,
            "experiment_label": experiment["label"],
            "hypothesis": experiment["hypothesis"],
            "query_rotation": experiment["query_rotation"],
            "sample_target": experiment_sample_target(forced),
            "source": "env",
            "week_start": week_start.isoformat(),
            "week_start_date": week_start.date().isoformat(),
        }

    with _session() as session:
        plan = _current_week_plan_payload(session, week_start) or _latest_plan_payload(session)

    if plan and str(plan.get("experiment_variant")) in EXPERIMENTS:
        plan["source"] = "weekly_plan"
        plan.setdefault("sample_target", experiment_sample_target(plan))
        return plan

    experiment = EXPERIMENTS[DEFAULT_EXPERIMENT_VARIANT]
    return {
        "experiment_variant": DEFAULT_EXPERIMENT_VARIANT,
        "experiment_label": experiment["label"],
        "hypothesis": experiment["hypothesis"],
        "query_rotation": experiment["query_rotation"],
        "sample_target": experiment_sample_target(DEFAULT_EXPERIMENT_VARIANT),
        "source": "default",
        "week_start": week_start.isoformat(),
        "week_start_date": week_start.date().isoformat(),
    }


def active_relay_experiment_variant() -> str:
    variant = str(active_relay_experiment().get("experiment_variant") or DEFAULT_EXPERIMENT_VARIANT)
    return variant if variant in EXPERIMENTS else DEFAULT_EXPERIMENT_VARIANT


def active_relay_query_hint(now: datetime | None = None) -> str | None:
    plan = active_relay_experiment()
    queries = [str(item).strip() for item in plan.get("query_rotation", []) if str(item).strip()]
    if not queries:
        return None
    now = now or _now()
    return queries[(now.timetuple().tm_yday + now.hour) % len(queries)]


def relay_performance_status() -> dict[str, Any]:
    now = _now()
    week_start = _week_start(now)
    with _session() as session:
        current_week = _metrics_for_window(session, start=week_start, end=now)
        prior_week = _metrics_for_window(
            session,
            start=week_start - timedelta(days=7),
            end=week_start,
        )
        rolling_window = _metrics_for_window(session, start=now - timedelta(days=7), end=now)
        all_time_sends = _event_count(session, "custom_outreach_sent_step_%", like=True)
        all_time_replies = (
            _event_count(session, "custom_outreach_reply_seen")
            + _event_count(session, "smartlead_reply")
        )
        latest_plan = _latest_plan_payload(session)
        latest_review = _event_payload(_latest_event(session, PERFORMANCE_REVIEW_EVENT))
        research = _event_payload(_latest_event(session, RESEARCH_INPUT_EVENT))
        prospect_health = _prospect_health(session)
        active_plan = active_relay_experiment()
        active_variant = str(active_plan.get("experiment_variant") or DEFAULT_EXPERIMENT_VARIANT)
        active_start, active_start_basis = _experiment_start(active_plan, now - timedelta(days=7))
        active_signal = _variant_metrics_for_window(
            session,
            variant=active_variant,
            start=active_start,
            end=now,
        )

    return {
        "status": "ok",
        "version": CURRENT_VERSION,
        "current_week": current_week,
        "prior_week": prior_week,
        "rolling_7_day": rolling_window,
        "all_time": {
            "sends": all_time_sends,
            "replies": all_time_replies,
            "reply_rate": round(all_time_replies / all_time_sends, 4) if all_time_sends else 0,
        },
        "prospect_health": prospect_health,
        "active_experiment": active_plan,
        "active_experiment_signal": active_signal,
        "active_experiment_signal_window": {
            "start": active_start.isoformat(),
            "end": now.isoformat(),
            "basis": active_start_basis,
        },
        "latest_review": latest_review,
        "latest_research_input": research,
        "available_experiments": EXPERIMENTS,
    }
