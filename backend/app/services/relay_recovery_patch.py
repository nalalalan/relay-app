from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import func, select

from app.api.admin_auth import require_relay_admin
from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.apollo import ApolloClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.services.custom_outreach import StepTemplate


router = APIRouter()

GENERIC_INBOX_LOCAL_PARTS = {
    "admin",
    "contact",
    "hello",
    "hi",
    "info",
    "inquiries",
    "mail",
    "marketing",
    "office",
    "sales",
    "support",
    "team",
}

RECOVERY_STEP_TEMPLATES = [
    StepTemplate(
        step_number=1,
        subject="after-call follow-up",
        body=(
            "Hey - quick question.\n\n"
            "When a good sales or client call ends, does your team already have someone who turns the messy notes into the recap, follow-up email, next steps, and CRM update the same day?\n\n"
            "I built Relay for that after-call cleanup. No software setup - you send rough notes, and the finished handoff comes back ready to use.\n\n"
            "Worth sending the sample?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=0,
    ),
    StepTemplate(
        step_number=2,
        subject="re: after-call follow-up",
        body=(
            "Following up once with the concrete version.\n\n"
            "Sample packet:\n"
            "{sample_url}\n\n"
            "The use case is simple: send rough notes from one real call, get back the client-ready recap, follow-up draft, open questions, and CRM-ready update.\n\n"
            "If you have one messy call from this week, I can turn it around as a $40 test.\n\n"
            "- Alan"
        ),
        delay_after_prev_days=1,
    ),
    StepTemplate(
        step_number=3,
        subject="re: after-call follow-up",
        body=(
            "Last note from me.\n\n"
            "If after-call follow-up is a real bottleneck, the lowest-friction test is one call for $40:\n"
            "{packet_checkout_url}\n\n"
            "More detail is here:\n"
            "{landing_page_url}\n\n"
            "If it is not relevant, no worries - I will not keep chasing.\n\n"
            "- Alan"
        ),
        delay_after_prev_days=2,
    ),
]

_original_apollo_search = None
_original_outreach_status = None
_money_loop_task: asyncio.Task | None = None
_money_loop_state: dict[str, Any] = {
    "enabled": False,
    "running": False,
    "last_tick_at": "",
    "last_result": None,
    "last_manual_kick_at": "",
    "last_manual_result": None,
    "last_error": "",
    "ticks": 0,
}

ACTIVE_OUTREACH_STATUSES = ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]


def _split_csv(value: str) -> list[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def _body_bool(body: dict[str, Any], key: str, default: bool = False) -> bool:
    value = body.get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _body_int(body: dict[str, Any], key: str, default: int, *, minimum: int = 1, maximum: int = 100) -> int:
    try:
        value = int(body.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _landing_page_url() -> str:
    url = os.getenv("LANDING_PAGE_URL", "").strip() or settings.landing_page_url.strip()
    if not url or "nalalalan.github.io/alan-operator-site" in url:
        return "https://relay.aolabs.io"
    return url.rstrip("/")


def _sample_url() -> str:
    return _landing_page_url().rstrip("/") + "/sample.pdf"


def _is_generic_inbox(email_address: str) -> bool:
    local = (email_address or "").split("@", 1)[0].strip().lower()
    if not local:
        return True
    local_base = local.replace(".", "").replace("-", "").replace("_", "")
    if local in GENERIC_INBOX_LOCAL_PARTS or local_base in GENERIC_INBOX_LOCAL_PARTS:
        return True
    return local.startswith(("info", "hello", "contact", "admin", "support", "sales"))


def _generic_policy() -> str:
    raw = os.getenv("COLD_OUTREACH_GENERIC_POLICY", "direct_only").strip().lower()
    if raw in {"include", "fallback", "direct_only"}:
        return raw
    return "direct_only"


def _allow_generic_imports() -> bool:
    return os.getenv("ACQ_IMPORT_GENERIC_INBOXES", "").strip().lower() in {"1", "true", "yes"}


def _prospect_priority(prospect: AcquisitionProspect) -> tuple[int, int, int, datetime]:
    band_rank = {"strong": 0, "good": 1, "maybe": 2}.get((prospect.fit_band or "").lower(), 9)
    created = prospect.created_at or datetime.min
    return (
        1 if _is_generic_inbox(prospect.contact_email) else 0,
        band_rank,
        -int(prospect.fit_score or 0),
        created,
    )


def _render_body(template: StepTemplate, prospect: AcquisitionProspect) -> str:
    body = template.body.format(
        company_name=prospect.company_name or "there",
        contact_name=prospect.contact_name or "",
        packet_offer_name=settings.packet_offer_name,
        packet_checkout_url=settings.packet_checkout_url,
        landing_page_url=_landing_page_url(),
        sample_url=_sample_url(),
    )
    return body.strip()


def _patched_outreach_status() -> dict[str, Any]:
    assert _original_outreach_status is not None
    status = _original_outreach_status()

    with SessionLocal() as session:
        quality = _quality_snapshot(session)

    status.update(quality)
    status["raw_due_now_count"] = int(status.get("due_now_count") or 0)
    status["due_now_count"] = quality["sendable_due_count"]
    status["queued_count"] = quality["sendable_due_count"]
    status["generic_send_policy"] = _generic_policy()
    status["quality_mode"] = "direct inboxes first; generic inboxes paused unless policy changes"
    status["money_loop"] = dict(_money_loop_state)
    status["next_money_move"] = _next_money_move(status)
    status["money_target"] = _money_target_snapshot(status)
    return status


def _total_send_count(session) -> int:
    count = session.execute(
        select(func.count(AcquisitionEvent.id)).where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
    ).scalar()
    return int(count or 0)


def _money_target_snapshot(status: dict[str, Any]) -> dict[str, Any]:
    try:
        target_weekly_usd = int(os.getenv("RELAY_WEEKLY_TARGET_USD", "100") or 100)
    except ValueError:
        target_weekly_usd = 100
    try:
        test_price_usd = float(os.getenv("RELAY_PACKET_PRICE_USD", "40") or 40)
    except ValueError:
        test_price_usd = 40.0

    paid_tests_needed = max(1, int((target_weekly_usd + test_price_usd - 1) // test_price_usd))
    daily_cap = int(status.get("daily_send_cap") or settings.buyer_acq_daily_send_cap or 0)
    weekly_send_capacity = daily_cap * 5
    return {
        "weekly_target_usd": target_weekly_usd,
        "test_price_usd": test_price_usd,
        "paid_tests_needed_weekly": paid_tests_needed,
        "current_daily_send_cap": daily_cap,
        "business_week_send_capacity": weekly_send_capacity,
        "operating_mode": "direct decision-maker inboxes only; generic inboxes are paused",
    }


def _compact_status_for_loop(status: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "queued_count",
        "due_now_count",
        "in_sequence_count",
        "sent_today",
        "replies_today",
        "daily_send_cap",
        "send_window_is_open",
        "direct_inbox_count",
        "generic_inbox_count",
        "direct_due_count",
        "generic_due_count",
        "sendable_due_count",
        "generic_paused_count",
        "cap_remaining",
        "total_sends_all_time",
        "next_money_move",
        "money_target",
        "active_experiment_variant",
        "active_experiment_sends",
        "active_experiment_sample_target",
        "active_experiment_needs_sample",
        "active_experiment_new_due_count",
        "blocked_bad_email_count",
        "reply_autoclose_mode",
    ]
    return {key: status.get(key) for key in keys if key in status}


def _compact_outreach_result(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    compact = dict(result)
    if isinstance(compact.get("status"), dict):
        compact["status"] = _compact_status_for_loop(compact["status"])
    return compact


def _status_label(value: Any) -> str:
    if not isinstance(value, dict):
        return str(value or "unknown")[:80]
    status = value.get("status")
    if isinstance(status, str) and status.strip():
        label = status.strip()
        if label in {"degraded_ok", "error"}:
            details: list[str] = []
            reason = str(value.get("reason") or "").strip()
            if reason:
                details.append(reason)
            error_type = str(value.get("error_type") or "").strip()
            if error_type:
                details.append(f"error_type={error_type}")
            elif value.get("error"):
                details.append("error_present=1")
            fallback = value.get("fallback_result")
            if isinstance(fallback, dict):
                fallback_status = str(fallback.get("status") or "").strip()
                fallback_error_type = str(fallback.get("error_type") or "").strip()
                upserted = fallback.get("upserted")
                searched = fallback.get("searched")
                if fallback_status:
                    details.append(f"fallback={fallback_status}")
                if fallback_error_type:
                    details.append(f"fallback_error_type={fallback_error_type}")
                if upserted is not None:
                    details.append(f"upserted={upserted}")
                if searched is not None:
                    details.append(f"searched={searched}")
            if details:
                label = f"{label}:{','.join(details)}"
        return label[:160]
    if isinstance(status, dict):
        return "snapshot"
    reason = value.get("reason") or value.get("summary")
    if reason:
        return str(reason).strip()[:80]
    return "ok"


def _log_money_loop_tick(result: dict[str, Any]) -> None:
    try:
        summary = (
            f"refill={_status_label(result.get('refill_result'))} "
            f"outreach={_status_label(result.get('outreach_result'))} "
            f"success={_status_label(result.get('success_control'))}"
        )
        with SessionLocal() as session:
            session.add(
                AcquisitionEvent(
                    event_type="relay_money_loop_tick",
                    prospect_external_id="relay-money-loop",
                    summary=summary[:500],
                    payload_json=json.dumps(result, ensure_ascii=False),
                )
            )
            session.commit()
    except Exception as exc:
        _money_loop_state["last_error"] = f"money_loop_log_failed: {exc}"


def _quality_snapshot(session) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    prospects = list(
        session.execute(
            select(AcquisitionProspect)
            .where(AcquisitionProspect.contact_email != "")
            .where(AcquisitionProspect.status.in_(ACTIVE_OUTREACH_STATUSES))
        ).scalars().all()
    )

    direct_active = 0
    generic_active = 0
    direct_due = 0
    generic_due = 0

    for prospect in prospects:
        is_generic = _is_generic_inbox(prospect.contact_email)
        if is_generic:
            generic_active += 1
        else:
            direct_active += 1

        if outreach._has_any_reply(session, prospect.external_id):
            continue
        sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
        if outreach._step_due(prospect, sent_events) is None:
            continue
        if is_generic:
            generic_due += 1
        else:
            direct_due += 1

    policy = _generic_policy()
    if policy == "include":
        sendable_due = direct_due + generic_due
        paused_generic = 0
    elif policy == "fallback" and direct_due == 0:
        sendable_due = generic_due
        paused_generic = 0
    else:
        sendable_due = direct_due
        paused_generic = generic_due

    daily_cap = int(settings.buyer_acq_daily_send_cap or 0)
    sent_today = int(outreach._daily_send_count(session) or 0)

    return {
        "direct_inbox_count": direct_active,
        "generic_inbox_count": generic_active,
        "direct_due_count": direct_due,
        "generic_due_count": generic_due,
        "sendable_due_count": sendable_due,
        "generic_paused_count": paused_generic,
        "cap_remaining": max(daily_cap - sent_today, 0),
        "total_sends_all_time": _total_send_count(session),
    }


def _next_money_move(status: dict[str, Any]) -> str:
    if int(status.get("replies_today") or 0) > 0:
        return "Handle replies first; real humans are the closest money."
    if int(status.get("direct_due_count") or 0) > 0 and int(status.get("cap_remaining") or 0) > 0:
        if status.get("send_window_is_open"):
            return "Send direct-inbox leads now; keep generic inboxes paused."
        return "Direct leads are ready; wait for the send window."
    if int(status.get("generic_paused_count") or 0) > 0:
        return "Generic inboxes are paused; refill with Apollo people leads."
    return "Refill direct decision-maker leads before increasing volume."


def _patched_send_due_sequence_messages(limit: int | None = None) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    limit = limit or settings.buyer_acq_daily_send_cap
    sent = 0
    skipped = 0
    failures: list[dict[str, Any]] = []

    window = outreach._send_window_status()
    if not window["is_open"]:
        return {
            "status": "ok",
            "summary": "outside_send_window",
            "sent_count": 0,
            "skipped_count": 0,
            "failures": [],
            "send_window": window,
        }

    with SessionLocal() as session:
        prospects = list(
            session.execute(
                select(AcquisitionProspect)
                .where(AcquisitionProspect.contact_email != "")
                .where(AcquisitionProspect.status.in_(ACTIVE_OUTREACH_STATUSES))
            ).scalars().all()
        )
        prospects.sort(key=_prospect_priority)

        direct_due: list[tuple[AcquisitionProspect, Any]] = []
        generic_due: list[tuple[AcquisitionProspect, Any]] = []

        for prospect in prospects:
            if outreach._has_any_reply(session, prospect.external_id):
                continue
            sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
            step = outreach._step_due(prospect, sent_events)
            if step is None:
                skipped += 1
                continue
            if _is_generic_inbox(prospect.contact_email):
                generic_due.append((prospect, step))
            else:
                direct_due.append((prospect, step))

        policy = _generic_policy()
        if policy == "include":
            candidates = direct_due + generic_due
            paused_generic = 0
        elif policy == "fallback" and not direct_due:
            candidates = generic_due
            paused_generic = 0
        else:
            candidates = direct_due
            paused_generic = len(generic_due)

        remaining_cap = max(settings.buyer_acq_daily_send_cap - outreach._daily_send_count(session), 0)

        for prospect, step in candidates:
            if sent >= limit or remaining_cap <= 0:
                break

            try:
                plain_text = outreach._render_body(step, prospect)
                html_body = plain_text.replace("\n", "<br>")
                result = outreach._outbound_send(
                    to_email=prospect.contact_email,
                    subject=step.subject,
                    plain_text=plain_text,
                    html_body=html_body,
                )
                outreach._log_event(
                    session,
                    f"custom_outreach_sent_step_{step.step_number}",
                    prospect.external_id,
                    f"sent custom outreach step {step.step_number}",
                    {
                        "to_email": prospect.contact_email,
                        "subject": step.subject,
                        "step_number": step.step_number,
                        "company_name": prospect.company_name,
                        "contact_name": prospect.contact_name,
                        "fit_score": prospect.fit_score,
                        "fit_band": prospect.fit_band,
                        "is_generic_inbox": _is_generic_inbox(prospect.contact_email),
                        "quality_gate": "direct_first",
                        "body_preview": outreach._preview_text(plain_text, limit=240),
                        **result,
                    },
                )
                prospect.status = "sent_custom"
                session.commit()
                sent += 1
                remaining_cap -= 1
            except Exception as exc:
                failures.append({"external_id": prospect.external_id, "error": str(exc)})
                outreach._log_event(
                    session,
                    "custom_outreach_send_failed",
                    prospect.external_id,
                    "custom outreach send failed",
                    {"error": str(exc), "step_number": step.step_number},
                )
                session.commit()

        quality = _quality_snapshot(session)

    return {
        "status": "ok",
        "summary": "direct_quality_gate",
        "sent_count": sent,
        "skipped_count": skipped + paused_generic,
        "failures": failures,
        "quality_gate": {
            "policy": policy,
            "direct_due_count": len(direct_due),
            "generic_due_count": len(generic_due),
            "generic_paused_count": paused_generic,
            "cap_remaining_after": quality["cap_remaining"],
        },
    }


def _patched_run_custom_outreach_cycle() -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    send_result = _patched_send_due_sequence_messages()
    reply_result = outreach.poll_reply_mailbox()
    return {
        "send_result": send_result,
        "reply_result": reply_result,
        "status": _patched_outreach_status(),
    }


async def import_from_apollo_people_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    import app.services.acquisition_supervisor as acq

    client = ApolloClient()
    raw_apollo_payload = payload.get("apollo_payload")
    search_payload: Dict[str, Any] = dict(raw_apollo_payload) if isinstance(raw_apollo_payload, dict) else {}
    q_keywords = str(payload.get("q_keywords") or "").strip()

    search_payload.setdefault("page", int(payload.get("page") or 1))
    search_payload.setdefault("per_page", max(1, min(int(payload.get("per_page") or 25), 100)))
    search_payload.setdefault("person_titles", payload.get("person_titles") or _split_csv(settings.acq_target_person_titles))
    search_payload.setdefault("organization_locations", payload.get("organization_locations") or [settings.default_country])
    search_payload.setdefault("contact_email_status", payload.get("contact_email_status") or ["verified", "guessed"])
    if q_keywords:
        search_payload.setdefault("q_keywords", q_keywords)

    result = await client.search_people(search_payload)
    rows = acq._extract_people_rows(result)

    with acq._session() as session:
        count = 0
        skipped_generic = 0
        for person in rows:
            organization = person.get("organization") or {}
            email = person.get("email") or person.get("contact_email") or ""
            if not email:
                continue
            if _is_generic_inbox(email) and not _allow_generic_imports():
                skipped_generic += 1
                continue

            acq._upsert_prospect(
                session,
                {
                    **person,
                    "id": person.get("id") or person.get("person_id") or email,
                    "person_id": person.get("id") or person.get("person_id") or "",
                    "company_name": organization.get("name") or person.get("company_name") or "",
                    "website": organization.get("website_url") or person.get("website_url") or person.get("website") or "",
                    "title": person.get("title") or "",
                    "headline": person.get("headline") or q_keywords,
                    "email": email,
                    "source": "apollo_people",
                },
            )
            count += 1
        session.commit()

    return {
        "status": "ok",
        "source": "apollo_people",
        "searched": len(rows),
        "upserted": count,
        "skipped_generic_inboxes": skipped_generic,
        "apollo_payload": {
            key: value
            for key, value in search_payload.items()
            if key not in {"api_key", "password", "token"}
        },
    }


async def import_from_apollo_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    source = str(payload.get("source") or os.getenv("ACQ_OPS_SOURCE", "apollo_people")).strip()
    if source == "apollo_people" and settings.apollo_api_key:
        return await import_from_apollo_people_search(payload)
    assert _original_apollo_search is not None
    return await _original_apollo_search(payload)


@router.post("/apollo-people-search")
def apollo_people_search(
    body: dict,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_relay_admin),
) -> dict:
    background_tasks.add_task(_run_apollo_people_search, body)
    return {"status": "accepted"}


def _run_apollo_people_search(body: dict) -> None:
    try:
        asyncio.run(import_from_apollo_people_search(body))
    except Exception as exc:
        print("apollo_people_search error:", exc)


@router.get("/money-loop-status")
def money_loop_status(_: None = Depends(require_relay_admin)) -> dict:
    return dict(_money_loop_state)


@router.post("/money-kick")
async def money_kick(
    body: dict[str, Any] | None = None,
    _: None = Depends(require_relay_admin),
) -> dict[str, Any]:
    body = body or {}
    result = await _relay_money_loop_tick(
        force_refill=_body_bool(body, "force_refill", True),
        refill_query=str(body.get("q_keywords") or "").strip() or None,
        refill_per_page=_body_int(
            body,
            "per_page",
            int(os.getenv("AO_RELAY_REFILL_PER_PAGE", "50") or 50),
            minimum=1,
            maximum=100,
        ),
        send_live=_body_bool(body, "send_live", True),
    )
    _money_loop_state["last_manual_kick_at"] = datetime.now(timezone.utc).isoformat()
    _money_loop_state["last_manual_result"] = result
    return result


async def _relay_money_loop_tick(
    *,
    force_refill: bool = False,
    refill_query: str | None = None,
    refill_per_page: int | None = None,
    send_live: bool = True,
) -> dict[str, Any]:
    import app.services.autonomous_ops as ops
    import app.services.custom_outreach as outreach

    status = await asyncio.to_thread(outreach.outreach_status)
    direct_due = int(status.get("direct_due_count") or 0)
    active_experiment_needs_sample = bool(status.get("active_experiment_needs_sample"))
    active_experiment_new_due = int(status.get("active_experiment_new_due_count") or 0)
    refill_due = active_experiment_new_due if active_experiment_needs_sample else direct_due
    cap_remaining = int(status.get("cap_remaining") or 0)
    min_direct_due = int(os.getenv("AO_RELAY_MIN_DIRECT_DUE", str(max(settings.buyer_acq_daily_send_cap, 10))) or 10)

    if send_live:
        try:
            from app.services.relay_success_controller import run_relay_success_control_tick

            success_control = await asyncio.to_thread(run_relay_success_control_tick)
        except Exception as exc:
            success_control = {
                "status": "error",
                "reason": "success_control_failed",
                "error": str(exc),
            }
    else:
        success_control = {
            "status": "skipped",
            "reason": "send_live_false",
        }

    refill_result: dict[str, Any] = {"status": "skipped", "reason": "direct_due_ok"}
    if not settings.apollo_api_key:
        refill_result = {"status": "skipped", "reason": "missing_apollo_api_key"}
    elif force_refill or refill_due < min_direct_due:
        query = refill_query or ops.choose_query()
        importer = getattr(ops, "import_from_apollo_people_search", import_from_apollo_people_search)
        try:
            refill_result = await importer(
                {
                    "q_keywords": query,
                    "per_page": refill_per_page or int(os.getenv("AO_RELAY_REFILL_PER_PAGE", "50") or 50),
                }
            )
        except Exception as exc:
            refill_result = {
                "status": "error",
                "reason": "apollo_refill_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "q_keywords": query,
            }
            if _original_apollo_search is not None:
                try:
                    fallback_result = await _original_apollo_search({"q_keywords": query, "source": "apify"})
                    refill_result = {
                        "status": "degraded_ok",
                        "reason": "apollo_refill_failed_apify_fallback_ran",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "q_keywords": query,
                        "fallback_result": fallback_result,
                    }
                except Exception as fallback_exc:
                    refill_result["fallback_result"] = {
                        "status": "error",
                        "reason": "apify_fallback_failed",
                        "error_type": type(fallback_exc).__name__,
                        "error": str(fallback_exc),
                    }

    if send_live:
        outreach_result = _compact_outreach_result(await asyncio.to_thread(outreach.run_custom_outreach_cycle))
    else:
        outreach_result = {
            "status": "skipped",
            "reason": "send_live_false",
            "snapshot": _compact_status_for_loop(await asyncio.to_thread(outreach.outreach_status)),
        }
    result = {
        "refill_result": refill_result,
        "outreach_result": outreach_result,
        "success_control": success_control,
        "success_control_phase": "before_refill",
        "direct_due_before": direct_due,
        "active_experiment_needs_sample": active_experiment_needs_sample,
        "active_experiment_new_due_before": active_experiment_new_due,
        "refill_due_before": refill_due,
        "cap_remaining_before": cap_remaining,
        "force_refill": force_refill,
        "send_live": send_live,
        "status_after": _compact_status_for_loop(await asyncio.to_thread(outreach.outreach_status)),
    }
    _log_money_loop_tick(result)
    return result


async def _relay_money_loop() -> None:
    interval = max(int(os.getenv("AO_RELAY_MONEY_LOOP_INTERVAL_SECONDS", "900") or 900), 120)
    startup_delay = max(int(os.getenv("AO_RELAY_MONEY_LOOP_STARTUP_DELAY_SECONDS", "30") or 30), 5)
    await asyncio.sleep(startup_delay)
    while True:
        try:
            _money_loop_state["running"] = True
            _money_loop_state["enabled"] = True
            _money_loop_state["last_tick_at"] = datetime.now(timezone.utc).isoformat()
            _money_loop_state["last_result"] = await _relay_money_loop_tick()
            _money_loop_state["last_error"] = ""
            _money_loop_state["ticks"] = int(_money_loop_state.get("ticks") or 0) + 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _money_loop_state["last_error"] = str(exc)
        finally:
            _money_loop_state["running"] = False

        await asyncio.sleep(interval)


def start_relay_money_loop() -> None:
    global _money_loop_task
    enabled = os.getenv("AO_RELAY_MONEY_LOOP_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
    _money_loop_state["enabled"] = enabled
    if not enabled or (_money_loop_task is not None and not _money_loop_task.done()):
        return
    _money_loop_task = asyncio.create_task(_relay_money_loop())


async def stop_relay_money_loop() -> None:
    global _money_loop_task
    if _money_loop_task is None:
        return
    _money_loop_task.cancel()
    try:
        await _money_loop_task
    except asyncio.CancelledError:
        pass
    _money_loop_task = None


def apply_relay_recovery_patch() -> None:
    global _original_apollo_search, _original_outreach_status

    import app.api.routes.acquisition_supervisor as acq_route
    import app.api.routes.custom_outreach as outreach_route
    import app.services.acquisition_supervisor as acq
    import app.services.autonomous_ops as ops
    import app.services.custom_outreach as outreach

    if _original_apollo_search is None:
        _original_apollo_search = acq.import_from_apollo_search
    if _original_outreach_status is None:
        _original_outreach_status = outreach.outreach_status

    acq.import_from_apollo_search = import_from_apollo_search
    acq.import_from_apollo_people_search = import_from_apollo_people_search
    acq_route.import_from_apollo_search = import_from_apollo_search
    ops.import_from_apollo_search = import_from_apollo_search

    outreach.STEP_TEMPLATES = RECOVERY_STEP_TEMPLATES
    outreach._landing_page_url = _landing_page_url
    outreach._render_body = _render_body
    outreach.send_due_sequence_messages = _patched_send_due_sequence_messages
    outreach.run_custom_outreach_cycle = _patched_run_custom_outreach_cycle
    outreach.outreach_status = _patched_outreach_status
    outreach_route.send_due_sequence_messages = _patched_send_due_sequence_messages
    outreach_route.run_custom_outreach_cycle = _patched_run_custom_outreach_cycle
    outreach_route.outreach_status = _patched_outreach_status
