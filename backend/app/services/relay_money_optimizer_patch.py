from __future__ import annotations

import os
import json
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Callable

from sqlalchemy import func, select

from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.apollo import ApolloClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.services.custom_outreach import StepTemplate


ACTIVE_OUTREACH_STATUSES = ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
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
PLACEHOLDER_EMAILS = {
    "example@mail.com",
    "user@domain.com",
    "jane@email.com",
    "john@email.com",
    "test@example.com",
}
PLACEHOLDER_EMAIL_DOMAINS = {
    "domain.com",
    "example.com",
    "example.net",
    "example.org",
    "invalid.com",
    "test.com",
}
PLACEHOLDER_LOCAL_PARTS = {
    "demo",
    "email",
    "example",
    "firstname",
    "hello-world",
    "jane",
    "john",
    "lastname",
    "mail",
    "name",
    "sample",
    "test",
    "user",
    "your-email",
    "your_email",
    "yourname",
}
BUYER_TITLE_TERMS = {
    "account director",
    "agency owner",
    "ceo",
    "client services",
    "co-founder",
    "founder",
    "head of client",
    "head of growth",
    "managing partner",
    "owner",
    "partner",
    "president",
    "principal",
    "revenue",
    "sales",
    "vp",
}

OPTIMIZED_STEP_TEMPLATES = [
    StepTemplate(
        step_number=1,
        subject="client call recaps",
        body=(
            "Hey - quick question.\n\n"
            "I built Relay to turn rough notes from one client or sales call into the finished recap, follow-up draft, next steps, and CRM-ready update.\n\n"
            "Sample:\n"
            "{sample_url}\n\n"
            "Is that cleanup painful enough at {company_name} to test on one real call, or not really?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=0,
    ),
    StepTemplate(
        step_number=2,
        subject="re: client call recaps",
        body=(
            "Following up once.\n\n"
            "The useful part is not more software. It is getting the post-call cleanup finished when the team is busy.\n\n"
            "If you have one messy call from this week, I can turn it into a polished handoff as a $40 test.\n\n"
            "Worth doing, or should I close the loop?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=1,
    ),
    StepTemplate(
        step_number=3,
        subject="re: client call recaps",
        body=(
            "Last note from me.\n\n"
            "If after-call cleanup is not a real bottleneck, no worries.\n\n"
            "If one call is worth testing, the $40 checkout is here:\n"
            "{packet_checkout_url}\n\n"
            "Either way, this is my last email unless you reply.\n\n"
            "- Alan"
        ),
        delay_after_prev_days=2,
    ),
]

_applied = False
_original_outreach_status: Callable[[], dict[str, Any]] | None = None


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _active_experiment() -> dict[str, Any]:
    try:
        from app.services.relay_performance import active_relay_experiment

        experiment = active_relay_experiment()
    except Exception as exc:
        experiment = {
            "experiment_variant": "control_sample_ask",
            "experiment_label": "Control: sample ask",
            "source": "fallback",
            "error": str(exc),
        }

    variant = str(experiment.get("experiment_variant") or "control_sample_ask")
    experiment["experiment_variant"] = variant
    return experiment


def _templates_for_variant(variant: str) -> list[StepTemplate]:
    try:
        import app.services.custom_outreach as outreach

        variants = getattr(outreach, "STEP_TEMPLATE_VARIANTS", {}) or {}
        templates = variants.get(variant)
        if templates:
            return templates
    except Exception:
        pass
    return OPTIMIZED_STEP_TEMPLATES


def _prospect_variant(sent_events: list[Any], fallback_variant: str) -> str:
    for event in sent_events:
        payload = _safe_json(getattr(event, "payload_json", None))
        variant = str(payload.get("experiment_variant") or "").strip()
        if variant:
            return variant
    return fallback_variant or "control_sample_ask"


def _landing_page_url() -> str:
    url = os.getenv("LANDING_PAGE_URL", "").strip() or settings.landing_page_url.strip()
    if not url or "nalalalan.github.io/alan-operator-site" in url:
        return "https://relay.aolabs.io"
    return url.rstrip("/")


def _sample_url() -> str:
    return _landing_page_url().rstrip("/") + "/sample.pdf"


def _email_parts(email_address: str) -> tuple[str, str]:
    value = (email_address or "").strip().lower()
    if "@" not in value:
        return "", ""
    local, domain = value.rsplit("@", 1)
    return local.strip(), domain.strip()


def _normalized_email(email_address: str) -> str:
    local, domain = _email_parts(email_address)
    return f"{local}@{domain}" if local and domain else ""


def _local_base(value: str) -> str:
    return (value or "").strip().lower().replace(".", "").replace("-", "").replace("_", "")


def _is_generic_inbox(email_address: str) -> bool:
    local, _domain = _email_parts(email_address)
    if not local:
        return True
    local_base = _local_base(local)
    if local in GENERIC_INBOX_LOCAL_PARTS or local_base in GENERIC_INBOX_LOCAL_PARTS:
        return True
    return local.startswith(("info", "hello", "contact", "admin", "support", "sales", "media", "press"))


def _is_placeholder_email(email_address: str) -> bool:
    value = (email_address or "").strip().lower()
    local, domain = _email_parts(value)
    if not local or not domain or "." not in domain:
        return True
    local_base = _local_base(local)
    if value in PLACEHOLDER_EMAILS:
        return True
    if domain in PLACEHOLDER_EMAIL_DOMAINS:
        return True
    if local in PLACEHOLDER_LOCAL_PARTS or local_base in PLACEHOLDER_LOCAL_PARTS:
        return True
    if domain.endswith(".example") or domain.endswith(".invalid"):
        return True
    return False


def _has_human_contact_name(prospect: AcquisitionProspect) -> bool:
    contact = str(getattr(prospect, "contact_name", "") or "").strip().lower()
    company = str(getattr(prospect, "company_name", "") or "").strip().lower()
    if not contact or "@" in contact:
        return False
    if company and _local_base(contact) == _local_base(company):
        return False
    company_words = {"agency", "company", "marketing", "media", "group", "llc", "inc", "seo", "ads"}
    words = [part for part in contact.replace(",", " ").split() if part]
    if not words:
        return False
    if any(word in company_words for word in words):
        return False
    return any(char.isalpha() for char in contact)


def _is_human_decision_maker(prospect: AcquisitionProspect) -> bool:
    if _is_generic_inbox(prospect.contact_email):
        return False
    if _title_relevance(prospect) == 0:
        return True
    return _has_human_contact_name(prospect)


def _generic_policy() -> str:
    raw = os.getenv("COLD_OUTREACH_GENERIC_POLICY", "direct_only").strip().lower()
    if raw in {"include", "fallback", "direct_only"}:
        return raw
    return "direct_only"


def _allow_generic_imports() -> bool:
    return os.getenv("ACQ_IMPORT_GENERIC_INBOXES", "").strip().lower() in {"1", "true", "yes"}


def _title_relevance(prospect: AcquisitionProspect) -> int:
    text = " ".join(
        [
            str(getattr(prospect, "title", "") or ""),
            str(getattr(prospect, "segment", "") or ""),
            str(getattr(prospect, "contact_name", "") or ""),
        ]
    ).lower()
    if any(term in text for term in BUYER_TITLE_TERMS):
        return 0
    return 1


def _prospect_priority(prospect: AcquisitionProspect) -> tuple[int, int, int, int, int, int, datetime]:
    band_rank = {"strong": 0, "good": 1, "maybe": 2}.get((prospect.fit_band or "").lower(), 9)
    created = prospect.created_at or datetime.min
    return (
        1 if _is_placeholder_email(prospect.contact_email) else 0,
        1 if _is_generic_inbox(prospect.contact_email) else 0,
        0 if _has_human_contact_name(prospect) else 1,
        _title_relevance(prospect),
        band_rank,
        -int(prospect.fit_score or 0),
        created,
    )


def _sent_email_external_ids(session, email_address: str) -> set[str]:
    email = _normalized_email(email_address)
    if not email:
        return set()

    rows = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .where(AcquisitionEvent.payload_json.like(f"%{email}%"))
        ).scalars().all()
    )
    external_ids: set[str] = set()
    for row in rows:
        payload = _safe_json(row.payload_json)
        sent_to = _normalized_email(str(payload.get("to_email") or ""))
        if sent_to == email:
            external_ids.add(str(row.prospect_external_id or ""))
    return external_ids


def _is_duplicate_outreach_email(session, prospect: AcquisitionProspect) -> bool:
    external_ids = _sent_email_external_ids(session, prospect.contact_email)
    if not external_ids:
        return False
    return any(external_id and external_id != prospect.external_id for external_id in external_ids)


def _zero_reply_strict_mode(total_sends: int, total_replies: int) -> bool:
    return total_sends >= 100 and total_replies == 0


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
    blocked_bad_email = 0
    direct_due = 0
    generic_due = 0
    duplicate_email_due = 0
    weak_decision_maker_due = 0

    total_sends_all_time = int(outreach._total_send_count(session) or 0)
    total_replies_all_time = int(
        session.execute(
            select(func.count(AcquisitionEvent.id)).where(
                AcquisitionEvent.event_type.in_(
                    ["custom_outreach_reply_seen", "smartlead_reply"]
                )
            )
        ).scalar()
        or 0
    )
    strict_mode = _zero_reply_strict_mode(total_sends_all_time, total_replies_all_time)
    active_experiment = _active_experiment()
    active_variant = str(active_experiment.get("experiment_variant") or "control_sample_ask")

    for prospect in prospects:
        if _is_placeholder_email(prospect.contact_email):
            blocked_bad_email += 1
            continue

        is_generic = _is_generic_inbox(prospect.contact_email)
        if is_generic:
            generic_active += 1
        else:
            direct_active += 1

        if outreach._has_any_reply(session, prospect.external_id):
            continue
        sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
        prospect_variant = _prospect_variant(sent_events, active_variant)
        templates = _templates_for_variant(prospect_variant)
        if outreach._step_due(prospect, sent_events, templates=templates) is None:
            continue
        if _is_duplicate_outreach_email(session, prospect):
            duplicate_email_due += 1
            continue
        if strict_mode and not _is_human_decision_maker(prospect):
            weak_decision_maker_due += 1
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
        "blocked_bad_email_count": blocked_bad_email,
        "duplicate_email_due_count": duplicate_email_due,
        "weak_decision_maker_due_count": weak_decision_maker_due,
        "direct_due_count": direct_due,
        "generic_due_count": generic_due,
        "sendable_due_count": sendable_due,
        "generic_paused_count": paused_generic,
        "cap_remaining": max(daily_cap - sent_today, 0),
        "zero_reply_strict_mode": strict_mode,
        "total_sends_all_time": total_sends_all_time,
        "total_replies_all_time": total_replies_all_time,
    }


def _next_money_move(status: dict[str, Any]) -> str:
    total_sends = int(status.get("total_sends_all_time") or 0)
    total_replies = int(status.get("total_replies_all_time") or 0)
    if total_sends >= 100 and total_replies == 0:
        weak_due = int(status.get("weak_decision_maker_due_count") or 0)
        duplicate_due = int(status.get("duplicate_email_due_count") or 0)
        if weak_due or duplicate_due:
            return (
                f"No reply signal after {total_sends} sends. Relay is now suppressing "
                f"{weak_due} weak decision-maker leads and {duplicate_due} duplicate email leads; "
                "send only named direct buyers with the reply-first sample ask."
            )
        return (
            f"No reply signal after {total_sends} sends. Treat this as a targeting/copy problem: "
            "send only named direct buyers with the reply-first sample ask, and do not increase volume yet."
        )
    if int(status.get("replies_today") or 0) > 0:
        return "Handle replies first; real humans are closest to money."
    if int(status.get("blocked_bad_email_count") or 0) > 0:
        return "Bad placeholder emails are being blocked; keep capacity for real decision-maker inboxes."
    if int(status.get("sent_today") or 0) >= int(status.get("daily_send_cap") or 0) and int(status.get("replies_today") or 0) == 0:
        return "Cap is used with no replies today; do not scale volume until targeting/copy produces a reply signal."
    if int(status.get("direct_due_count") or 0) > 0 and int(status.get("cap_remaining") or 0) > 0:
        if status.get("send_window_is_open"):
            return "Send direct decision-maker leads now; keep generic inboxes paused."
        return "Direct decision-maker leads are ready; wait for the send window."
    if int(status.get("generic_paused_count") or 0) > 0:
        return "Generic inboxes are paused; refill with Apollo people leads."
    return "Refill direct decision-maker leads before increasing volume."


def _fallback_status(error: Exception) -> dict[str, Any]:
    daily_cap = int(settings.buyer_acq_daily_send_cap or 0)
    return {
        "queued_count": 0,
        "due_now_count": 0,
        "in_sequence_count": 0,
        "direct_inbox_count": 0,
        "generic_inbox_count": 0,
        "sent_today": 0,
        "total_sends_all_time": 0,
        "total_replies_all_time": 0,
        "replies_today": 0,
        "daily_send_cap": daily_cap,
        "recent_sent": [],
        "recent_replies": [],
        "active_experiment": _active_experiment(),
        "send_window_is_open": False,
        "status_warning": f"base_status_unavailable:{type(error).__name__}",
    }


def _fallback_quality(status: dict[str, Any], error: Exception) -> dict[str, Any]:
    sent_today = int(status.get("sent_today") or 0)
    daily_cap = int(status.get("daily_send_cap") or settings.buyer_acq_daily_send_cap or 0)
    due_now = int(status.get("due_now_count") or status.get("queued_count") or 0)
    return {
        "direct_inbox_count": int(status.get("direct_inbox_count") or 0),
        "generic_inbox_count": int(status.get("generic_inbox_count") or 0),
        "blocked_bad_email_count": int(status.get("blocked_bad_email_count") or 0),
        "duplicate_email_due_count": int(status.get("duplicate_email_due_count") or 0),
        "weak_decision_maker_due_count": int(status.get("weak_decision_maker_due_count") or 0),
        "direct_due_count": int(status.get("direct_due_count") or due_now),
        "generic_due_count": int(status.get("generic_due_count") or 0),
        "sendable_due_count": due_now,
        "generic_paused_count": int(status.get("generic_paused_count") or 0),
        "cap_remaining": int(status.get("cap_remaining") or max(daily_cap - sent_today, 0)),
        "total_replies_all_time": int(
            status.get("total_replies_all_time")
            or status.get("all_time_replies")
            or status.get("replies_today")
            or 0
        ),
        "zero_reply_strict_mode": bool(status.get("zero_reply_strict_mode") or False),
        "quality_snapshot_warning": type(error).__name__,
    }


def optimized_outreach_status() -> dict[str, Any]:
    assert _original_outreach_status is not None
    try:
        status = _original_outreach_status()
    except Exception as exc:
        status = _fallback_status(exc)

    try:
        with SessionLocal() as session:
            quality = _quality_snapshot(session)
    except Exception as exc:
        quality = _fallback_quality(status, exc)

    status.update(quality)
    status["raw_due_now_count"] = int(status.get("due_now_count") or 0)
    status["due_now_count"] = quality["sendable_due_count"]
    status["queued_count"] = quality["sendable_due_count"]
    status["generic_send_policy"] = _generic_policy()
    status["quality_mode"] = "direct decision-maker inboxes first; placeholder and generic inboxes blocked from normal sends"
    status["next_money_move"] = _next_money_move(status)
    return status


def optimized_send_due_sequence_messages(limit: int | None = None) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    limit = limit or settings.buyer_acq_daily_send_cap
    sent = 0
    skipped = 0
    failures: list[dict[str, Any]] = []
    active_experiment = _active_experiment()
    active_variant = str(active_experiment.get("experiment_variant") or "control_sample_ask")

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
        reserved_emails: set[str] = set()
        blocked_bad_email = 0
        duplicate_email_blocked = 0
        weak_decision_maker_blocked = 0
        total_sends_all_time = int(outreach._total_send_count(session) or 0)
        total_replies_all_time = int(
            session.execute(
                select(func.count(AcquisitionEvent.id)).where(
                    AcquisitionEvent.event_type.in_(
                        ["custom_outreach_reply_seen", "smartlead_reply"]
                    )
                )
            ).scalar()
            or 0
        )
        strict_mode = _zero_reply_strict_mode(total_sends_all_time, total_replies_all_time)

        for prospect in prospects:
            if _is_placeholder_email(prospect.contact_email):
                prospect.status = "manual_review"
                outreach._log_event(
                    session,
                    "custom_outreach_blocked_bad_email",
                    prospect.external_id,
                    "blocked placeholder or invalid email before send",
                    {
                        "to_email": prospect.contact_email,
                        "company_name": prospect.company_name,
                    },
                )
                blocked_bad_email += 1
                skipped += 1
                continue

            email = _normalized_email(prospect.contact_email)
            if email in reserved_emails or _is_duplicate_outreach_email(session, prospect):
                prospect.status = "manual_review"
                outreach._log_event(
                    session,
                    "custom_outreach_blocked_duplicate_email",
                    prospect.external_id,
                    "blocked duplicate outreach email before send",
                    {
                        "to_email": prospect.contact_email,
                        "company_name": prospect.company_name,
                        "contact_name": prospect.contact_name,
                    },
                )
                duplicate_email_blocked += 1
                skipped += 1
                continue

            if strict_mode and not _is_human_decision_maker(prospect):
                prospect.status = "manual_review"
                outreach._log_event(
                    session,
                    "custom_outreach_paused_weak_decision_maker",
                    prospect.external_id,
                    "paused weak decision-maker lead after zero-reply send signal",
                    {
                        "to_email": prospect.contact_email,
                        "company_name": prospect.company_name,
                        "contact_name": prospect.contact_name,
                        "title": prospect.title,
                        "fit_score": prospect.fit_score,
                        "fit_band": prospect.fit_band,
                    },
                )
                weak_decision_maker_blocked += 1
                skipped += 1
                continue

            if outreach._has_any_reply(session, prospect.external_id):
                continue
            sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
            prospect_variant = _prospect_variant(sent_events, active_variant)
            templates = _templates_for_variant(prospect_variant)
            step = outreach._step_due(prospect, sent_events, templates=templates)
            if step is None:
                skipped += 1
                continue
            if _is_generic_inbox(prospect.contact_email):
                generic_due.append((prospect, step))
            else:
                direct_due.append((prospect, step))
                if email:
                    reserved_emails.add(email)

        session.commit()

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
                sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
                prospect_variant = _prospect_variant(sent_events, active_variant)
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
                        "quality_gate": "direct_decision_maker_first",
                        "experiment_variant": prospect_variant,
                        "active_experiment_variant": active_variant,
                        "active_experiment_label": active_experiment.get("experiment_label"),
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
        "summary": "direct_decision_maker_quality_gate",
        "sent_count": sent,
        "skipped_count": skipped + paused_generic,
        "failures": failures,
        "active_experiment": active_experiment,
        "quality_gate": {
            "policy": policy,
            "direct_due_count": len(direct_due),
            "generic_due_count": len(generic_due),
            "generic_paused_count": paused_generic,
            "blocked_bad_email_count": blocked_bad_email,
            "blocked_duplicate_email_count": duplicate_email_blocked,
            "paused_weak_decision_maker_count": weak_decision_maker_blocked,
            "zero_reply_strict_mode": strict_mode,
            "cap_remaining_after": quality["cap_remaining"],
        },
    }


def optimized_run_custom_outreach_cycle() -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    send_result = optimized_send_due_sequence_messages()
    reply_result = outreach.poll_reply_mailbox()
    return {
        "send_result": send_result,
        "reply_result": reply_result,
        "status": optimized_outreach_status(),
    }


def _zero_touch_close_reply() -> str:
    return (
        "Yes - here is the sample packet:\n"
        f"{_sample_url()}\n\n"
        "The simple paid test is one real call for $40:\n"
        f"{settings.packet_checkout_url}\n\n"
        "After checkout, send one messy call note or recording link. I will turn it into the recap, follow-up draft, open questions, and CRM-ready update.\n\n"
        "- Alan"
    )


def optimized_auto_reply_text(reply_text: str) -> tuple[str, str | None]:
    text = (reply_text or "").lower()
    if any(term in text for term in ["unsubscribe", "remove me", "not interested", "no thanks", "stop", "wrong person"]):
        return "negative", None
    if any(term in text for term in ["sample", "example", "proof", "what do i get", "how does it work"]):
        return "sample_request", _zero_touch_close_reply()
    if any(term in text for term in ["price", "pricing", "cost", "how much", "pay", "checkout", "buy", "start"]):
        return "pricing", _zero_touch_close_reply()
    if any(term in text for term in ["yes", "interested", "send it", "tell me more", "sounds good"]):
        return "interested", _zero_touch_close_reply()
    return "zero_touch", _zero_touch_close_reply()


def optimized_send_test_email(to_email: str) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    try:
        if outreach._smtp_enabled():
            mailboxes = outreach._smtp_mailboxes()
            if not mailboxes:
                raise RuntimeError("No SMTP mailboxes configured")

            mailbox = mailboxes[0]
            host = os.getenv("COLD_SMTP_HOST", "smtp.porkbun.com").strip()
            port = int(os.getenv("COLD_SMTP_PORT", "587").strip() or "587")
            security = os.getenv("COLD_SMTP_SECURITY", "starttls").strip().lower()
            reply_to = os.getenv("COLD_SMTP_REPLY_TO", settings.reply_to_email or "").strip()

            message = EmailMessage()
            message["From"] = mailbox.address
            message["To"] = to_email
            message["Subject"] = "Relay test email"
            if reply_to:
                message["Reply-To"] = reply_to
            message.set_content("Relay test email from the production custom sender.\n\n- Alan")
            message.add_alternative(
                "<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.6'>"
                "Relay test email from the production custom sender.<br><br>- Alan"
                "</div>",
                subtype="html",
            )

            if security == "ssl":
                server = smtplib.SMTP_SSL(host, port, timeout=30)
            else:
                server = smtplib.SMTP(host, port, timeout=30)

            try:
                server.ehlo()
                if security == "starttls":
                    server.starttls()
                    server.ehlo()
                server.login(mailbox.address, mailbox.password)
                server.sendmail(mailbox.address, [to_email], message.as_string())
            finally:
                try:
                    server.quit()
                except Exception:
                    pass

            return {
                "provider": "smtp",
                "sender_address": mailbox.address,
                "smtp_host": host,
                "smtp_port": port,
                "smtp_security": security,
                "ignored_campaign_cap": True,
                "status": "sent",
            }

        return outreach._outbound_send(
            to_email=to_email,
            subject="Relay test email",
            plain_text="Relay test email from the production custom sender.\n\n- Alan",
            html_body="Relay test email from the production custom sender.<br><br>- Alan",
        )
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


async def optimized_import_from_apollo_people_search(payload: dict[str, Any]) -> dict[str, Any]:
    import app.services.acquisition_supervisor as acq

    client = ApolloClient()
    raw_apollo_payload = payload.get("apollo_payload")
    search_payload: dict[str, Any] = dict(raw_apollo_payload) if isinstance(raw_apollo_payload, dict) else {}
    q_keywords = str(payload.get("q_keywords") or "").strip()

    search_payload.setdefault("page", int(payload.get("page") or 1))
    search_payload.setdefault("per_page", max(1, min(int(payload.get("per_page") or 25), 100)))
    search_payload.setdefault(
        "person_titles",
        payload.get("person_titles")
        or [
            "Founder",
            "Co-Founder",
            "Owner",
            "CEO",
            "President",
            "Managing Partner",
            "Principal",
            "Partner",
            "Head of Client Services",
            "Account Director",
            "Sales Director",
            "Head of Growth",
            "Revenue Operations",
        ],
    )
    search_payload.setdefault(
        "person_seniorities",
        payload.get("person_seniorities") or ["owner", "founder", "c_suite", "partner", "vp", "head", "director"],
    )
    search_payload.setdefault("organization_locations", payload.get("organization_locations") or [settings.default_country])
    search_payload.setdefault("contact_email_status", payload.get("contact_email_status") or ["verified", "guessed"])
    if q_keywords:
        search_payload.setdefault("q_keywords", q_keywords)

    result = await client.search_people(search_payload)
    rows = acq._extract_people_rows(result)

    with acq._session() as session:
        count = 0
        skipped_bad = 0
        skipped_generic = 0
        for person in rows:
            organization = person.get("organization") or {}
            email = person.get("email") or person.get("contact_email") or ""
            if not email:
                continue
            if _is_placeholder_email(email):
                skipped_bad += 1
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
        "skipped_bad_emails": skipped_bad,
        "skipped_generic_inboxes": skipped_generic,
        "apollo_payload": {
            key: value
            for key, value in search_payload.items()
            if key not in {"api_key", "password", "token"}
        },
    }


def apply_relay_money_optimizer_patch() -> None:
    global _applied, _original_outreach_status
    if _applied:
        return

    import app.api.routes.acquisition_supervisor as acq_route
    import app.api.routes.custom_outreach as outreach_route
    import app.services.acquisition_supervisor as acq
    import app.services.autonomous_ops as ops
    import app.services.custom_outreach as outreach

    _original_outreach_status = outreach.outreach_status

    acq._auto_reply_text = optimized_auto_reply_text
    acq.import_from_apollo_people_search = optimized_import_from_apollo_people_search
    acq_route.import_from_apollo_people_search = optimized_import_from_apollo_people_search
    ops.import_from_apollo_people_search = optimized_import_from_apollo_people_search

    outreach.STEP_TEMPLATES = OPTIMIZED_STEP_TEMPLATES
    outreach.STEP_TEMPLATE_VARIANTS = {
        **(getattr(outreach, "STEP_TEMPLATE_VARIANTS", {}) or {}),
        "control_sample_ask": OPTIMIZED_STEP_TEMPLATES,
        "sample_first_plain": OPTIMIZED_STEP_TEMPLATES,
    }
    outreach._landing_page_url = _landing_page_url
    outreach._sample_url = _sample_url
    outreach._render_body = _render_body
    outreach._auto_reply_text = optimized_auto_reply_text
    outreach.send_due_sequence_messages = optimized_send_due_sequence_messages
    outreach.run_custom_outreach_cycle = optimized_run_custom_outreach_cycle
    outreach.outreach_status = optimized_outreach_status
    outreach.send_test_email = optimized_send_test_email

    outreach_route.send_due_sequence_messages = optimized_send_due_sequence_messages
    outreach_route.run_custom_outreach_cycle = optimized_run_custom_outreach_cycle
    outreach_route.outreach_status = optimized_outreach_status
    outreach_route.send_test_email = optimized_send_test_email

    _applied = True
