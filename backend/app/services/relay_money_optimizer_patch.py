from __future__ import annotations

import os
import json
import smtplib
from datetime import datetime, timedelta, timezone
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
        subject="client call follow-up",
        body=(
            "Hey - quick question.\n\n"
            "I built Relay to turn messy notes from one client or sales call into the finished recap, follow-up draft, next steps, and CRM-ready update.\n\n"
            "If one call is sitting half-finished at {company_name}, you can send the rough notes here and I will look at it:\n"
            "{notes_url}\n\n"
            "Sample:\n"
            "{sample_url}\n\n"
            "Worth testing on one real call, or not really?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=0,
    ),
    StepTemplate(
        step_number=2,
        subject="re: client call follow-up",
        body=(
            "Following up once.\n\n"
            "The useful part is not more software. It is getting the post-call cleanup finished when the team is busy.\n\n"
            "Relay can work from rough bullets, a transcript, or an ugly notes dump. The low-friction test is to send one real call here:\n"
            "{notes_url}\n\n"
            "If it is a fit, I can turn it around as a paid $40 packet.\n\n"
            "Worth doing, or should I close the loop?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=1,
    ),
    StepTemplate(
        step_number=3,
        subject="re: client call follow-up",
        body=(
            "Last note from me.\n\n"
            "If after-call cleanup is not a real bottleneck, no worries.\n\n"
            "If one call is worth testing, send the messy notes here:\n"
            "{notes_url}\n\n"
            "Or start the $40 paid packet directly here:\n"
            "{packet_checkout_url}\n\n"
            "Either way, this is my last email unless you reply.\n\n"
            "- Alan"
        ),
        delay_after_prev_days=2,
    ),
]

OPTIMIZED_STEP_TEMPLATE_VARIANTS = {
    "control_sample_ask": OPTIMIZED_STEP_TEMPLATES,
    "sample_first_plain": [
        StepTemplate(
            step_number=1,
            subject="client call follow-up sample",
            body=(
                "Hey - quick question.\n\n"
                "I built Relay to turn messy notes from one client or sales call into the finished recap, follow-up draft, next steps, and CRM-ready update.\n\n"
                "Sample packet:\n"
                "{sample_url}\n\n"
                "If one call is sitting half-finished at {company_name}, you can send the rough notes here:\n"
                "{notes_url}\n\n"
                "Worth testing on one real call, or not really?\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: client call follow-up sample",
            body=(
                "Following up once.\n\n"
                "The sample shows the format. The useful part is getting the actual post-call cleanup finished when the team is busy.\n\n"
                "Send one messy note here and I will look at whether it fits:\n"
                "{notes_url}\n\n"
                "If it is a fit, the paid one-call packet is $40.\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: client call follow-up sample",
            body=(
                "Last note from me.\n\n"
                "If the sample is not relevant, no worries.\n\n"
                "If one real call is worth testing, send rough notes here:\n"
                "{notes_url}\n\n"
                "Or start the $40 packet directly here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
    "pain_owner_direct": [
        StepTemplate(
            step_number=1,
            subject="who owns after-call cleanup?",
            body=(
                "Hey - quick question for {company_name}.\n\n"
                "After a strong client or sales call, who owns the recap, next steps, follow-up email, and CRM update?\n\n"
                "I built Relay for that cleanup. Your team sends rough notes, and the finished handoff comes back ready to use.\n\n"
                "If nobody clearly owns that job, you can send one messy note here and I will look at it:\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: who owns after-call cleanup?",
            body=(
                "Following up once with the concrete version.\n\n"
                "Relay is for the gap after a good call: the work is obvious, but nobody wants to turn rough notes into a clean client-ready handoff.\n\n"
                "Sample:\n"
                "{sample_url}\n\n"
                "Worth testing on one real call?\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: who owns after-call cleanup?",
            body=(
                "Last note from me.\n\n"
                "If post-call cleanup is already owned and fast, no worries.\n\n"
                "If one call is worth testing, send rough notes here:\n"
                "{notes_url}\n\n"
                "Or start the $40 packet here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
    "paid_test_explicit": [
        StepTemplate(
            step_number=1,
            subject="one-call cleanup test",
            body=(
                "Hey - quick question.\n\n"
                "I am testing a $40 done-for-you Relay: send rough notes from one sales or client call, get back the recap, follow-up draft, next steps, and CRM-ready update.\n\n"
                "Sample:\n"
                "{sample_url}\n\n"
                "If one messy call from this week is worth trying, send it here:\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: one-call cleanup test",
            body=(
                "Following up once.\n\n"
                "The test is intentionally small: one real call, $40, finished handoff back to you.\n\n"
                "If it saves even one delayed follow-up, it should be obvious quickly.\n\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: one-call cleanup test",
            body=(
                "Last note from me.\n\n"
                "If this is not relevant, no worries - I will not keep chasing.\n\n"
                "If it is relevant later, the sample is here:\n"
                "{sample_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
    "hard_paid_test_direct": [
        StepTemplate(
            step_number=1,
            subject="one-call follow-up cleanup",
            body=(
                "Hey - quick question.\n\n"
                "If {company_name} has one sales or client call where the follow-up is stuck in rough notes, I can turn it into the client-ready recap, follow-up draft, next steps, and CRM-ready update for $40.\n\n"
                "The paid test is here:\n"
                "{packet_checkout_url}\n\n"
                "If you want me to look at fit first, send the rough note here:\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: one-call follow-up cleanup",
            body=(
                "Following up once.\n\n"
                "This is not a software rollout. It is one messy call turned into a finished handoff for $40.\n\n"
                "If delayed follow-up is costing time, the smallest test is here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: one-call follow-up cleanup",
            body=(
                "Last note from me.\n\n"
                "If this is not a real problem, no worries.\n\n"
                "If one stuck call is worth cleaning up, the paid test is here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
    "stalled_opportunity_direct": [
        StepTemplate(
            step_number=1,
            subject="proposal follow-up cleanup",
            body=(
                "Hey - quick question.\n\n"
                "If {company_name} has one sales call where the next step is stuck in notes instead of getting sent, I can turn the rough notes into the follow-up email, recap, open questions, and CRM-ready update for $40.\n\n"
                "This is for one stalled opportunity, not a software rollout.\n\n"
                "Start the paid test:\n"
                "{packet_checkout_url}\n\n"
                "If you want me to check fit first, send the rough notes:\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: proposal follow-up cleanup",
            body=(
                "Following up once.\n\n"
                "The offer is simple: one stalled follow-up becomes a finished client-ready handoff.\n\n"
                "If the deal is worth more than $40, cleanup should not be the bottleneck.\n\n"
                "Paid test:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: proposal follow-up cleanup",
            body=(
                "Last note from me.\n\n"
                "If every follow-up is already fast, ignore this.\n\n"
                "If one opportunity is stuck because the notes are not cleaned up, the $40 test is here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
    "revenue_leak_direct": [
        StepTemplate(
            step_number=1,
            subject="delayed follow-up after calls",
            body=(
                "Hey - quick question.\n\n"
                "When a good sales or client call ends at {company_name}, do follow-ups ever sit because the recap, next steps, and CRM update are still rough notes?\n\n"
                "Relay is a $40 cleanup for one call: send rough notes, get the client-ready follow-up handoff back.\n\n"
                "If one delayed follow-up is worth fixing, the paid test is here:\n"
                "{packet_checkout_url}\n\n"
                "Or send the rough note here if you want me to check fit first:\n"
                "{notes_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: delayed follow-up after calls",
            body=(
                "Following up once.\n\n"
                "The value is simple: fewer good calls turning into slow follow-up because nobody has time to clean the notes.\n\n"
                "One-call cleanup test:\n"
                "{packet_checkout_url}\n\n"
                "Sample format:\n"
                "{sample_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: delayed follow-up after calls",
            body=(
                "Last note from me.\n\n"
                "If delayed follow-up is not a bottleneck, no worries.\n\n"
                "If one call is worth cleaning up, the $40 test is here:\n"
                "{packet_checkout_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
}

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


def _notes_url() -> str:
    return _landing_page_url().rstrip("/") + "/#send-notes"


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


def _experiment_sample_target(active_experiment: dict[str, Any] | None = None) -> int:
    if isinstance(active_experiment, dict):
        try:
            direct_target = int(active_experiment.get("sample_target") or 0)
            if direct_target > 0:
                return direct_target
        except Exception:
            pass
        try:
            from app.services.relay_performance import experiment_sample_target

            target = experiment_sample_target(active_experiment)
            if target > 0:
                return target
        except Exception:
            pass
    try:
        return max(int(os.getenv("RELAY_EXPERIMENT_FAILURE_SAMPLE", os.getenv("RELAY_EXPERIMENT_MIN_SAMPLE", "20")) or 20), 1)
    except Exception:
        return 20


def _parse_experiment_datetime(value: Any) -> datetime | None:
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


def _active_experiment_start(experiment: dict[str, Any]) -> datetime:
    for key in ("week_start", "week_start_date", "created_at", "logged_at"):
        parsed = _parse_experiment_datetime(experiment.get(key))
        if parsed is not None:
            return parsed
    return datetime.utcnow() - timedelta(days=7)


def _active_generic_sample_cap() -> int:
    try:
        return max(int(os.getenv("RELAY_ACTIVE_GENERIC_SAMPLE_DAILY_CAP", "2") or 2), 0)
    except Exception:
        return 2


def _fill_remaining_cap_after_active_sample() -> bool:
    return os.getenv("RELAY_FILL_REMAINING_CAP_AFTER_ACTIVE_SAMPLE", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }


def _effective_daily_cap(experiment: dict[str, Any] | None = None) -> int:
    configured_cap = max(int(settings.buyer_acq_daily_send_cap or 0), 1)
    try:
        experiment_cap = int((experiment or {}).get("daily_cap_recommendation") or configured_cap)
    except Exception:
        experiment_cap = configured_cap
    return max(min(experiment_cap, configured_cap), 1)


def _send_window_wait_text(status: dict[str, Any]) -> str:
    next_open = str(status.get("send_window_next_open_local") or "").strip()
    if not next_open:
        return "wait for the send window"
    if str(status.get("send_window_reason") or "").strip() == "weekend":
        return f"next business-day send window opens {next_open}"
    return f"next send window opens {next_open}"


def _event_variant(event: AcquisitionEvent) -> str:
    payload = _safe_json(getattr(event, "payload_json", None))
    return str(payload.get("experiment_variant") or "control_sample_ask").strip() or "control_sample_ask"


def _event_step_number(event: AcquisitionEvent) -> int:
    payload = _safe_json(getattr(event, "payload_json", None))
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
    payload = _safe_json(getattr(event, "payload_json", None))
    marker = payload.get("active_experiment_first_touch")
    if isinstance(marker, bool):
        return marker
    if marker is not None:
        return str(marker).strip().lower() in {"1", "true", "yes", "y"}
    return _event_step_number(event) == 1


def _active_variant_send_count(session, active_variant: str, experiment: dict[str, Any] | None = None) -> int:
    since = _active_experiment_start(experiment or {})
    events = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .where(AcquisitionEvent.created_at >= since)
        ).scalars().all()
    )
    return sum(1 for event in events if _event_variant(event) == active_variant)


def _active_variant_sample_send_count(session, active_variant: str, experiment: dict[str, Any] | None = None) -> int:
    since = _active_experiment_start(experiment or {})
    events = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .where(AcquisitionEvent.created_at >= since)
        ).scalars().all()
    )
    return sum(
        1
        for event in events
        if _event_variant(event) == active_variant and _event_is_first_touch_sample(event)
    )


def _render_body(template: StepTemplate, prospect: AcquisitionProspect) -> str:
    body = template.body.format(
        company_name=prospect.company_name or "there",
        contact_name=prospect.contact_name or "",
        packet_offer_name=settings.packet_offer_name,
        packet_checkout_url=settings.packet_checkout_url,
        landing_page_url=_landing_page_url(),
        sample_url=_sample_url(),
        notes_url=_notes_url(),
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
    active_variant_sends = _active_variant_send_count(session, active_variant, active_experiment)
    active_variant_sample_sends_observed = _active_variant_sample_send_count(session, active_variant, active_experiment)
    experiment_sample_target = _experiment_sample_target(active_experiment)
    active_variant_sample_sends = min(active_variant_sample_sends_observed, experiment_sample_target)
    active_sample_complete = active_variant_sample_sends >= experiment_sample_target
    active_experiment_direct_new_due = 0
    active_experiment_generic_new_due = 0

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
        step = outreach._step_due(prospect, sent_events, templates=templates)
        if step is None:
            continue
        if _is_duplicate_outreach_email(session, prospect):
            duplicate_email_due += 1
            continue
        if strict_mode and not _is_human_decision_maker(prospect):
            weak_decision_maker_due += 1
            continue
        is_active_experiment_new = prospect_variant == active_variant and not sent_events and step.step_number == 1
        if is_generic:
            generic_due += 1
            if is_active_experiment_new:
                active_experiment_generic_new_due += 1
        else:
            direct_due += 1
            if is_active_experiment_new:
                active_experiment_direct_new_due += 1

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

    daily_cap = _effective_daily_cap(_active_experiment())
    sent_today = int(outreach._daily_send_count(session) or 0)
    generic_sample_cap = _active_generic_sample_cap()
    active_experiment_allowed_generic_new_due = min(active_experiment_generic_new_due, generic_sample_cap)
    active_experiment_new_due_raw = active_experiment_direct_new_due + active_experiment_allowed_generic_new_due
    active_experiment_new_due = 0 if active_sample_complete else active_experiment_new_due_raw

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
        "effective_daily_cap": daily_cap,
        "zero_reply_strict_mode": strict_mode,
        "total_sends_all_time": total_sends_all_time,
        "total_replies_all_time": total_replies_all_time,
        "active_experiment_variant": active_variant,
        "active_experiment_started_at": _active_experiment_start(active_experiment).isoformat(),
        "active_experiment_sends": active_variant_sends,
        "active_experiment_sample_sends": active_variant_sample_sends,
        "active_experiment_sample_sends_observed": active_variant_sample_sends_observed,
        "active_experiment_sample_target": experiment_sample_target,
        "active_experiment_needs_sample": not active_sample_complete,
        "active_experiment_new_due_count": active_experiment_new_due,
        "active_experiment_blocked_after_sample_complete_count": active_experiment_new_due_raw
        if active_sample_complete
        else 0,
        "active_experiment_direct_new_due_count": active_experiment_direct_new_due,
        "active_experiment_generic_new_due_count": active_experiment_generic_new_due,
        "active_experiment_allowed_generic_new_due_count": active_experiment_allowed_generic_new_due,
        "active_experiment_generic_sample_daily_cap": generic_sample_cap,
    }


def _next_money_move(status: dict[str, Any]) -> str:
    total_sends = int(status.get("total_sends_all_time") or 0)
    total_replies = int(status.get("total_replies_all_time") or 0)
    if int(status.get("replies_today") or 0) > 0:
        return "Handle replies first; real humans are closest to money."
    if int(status.get("blocked_bad_email_count") or 0) > 0:
        return "Bad placeholder emails are being blocked; keep capacity for real decision-maker inboxes."
    if status.get("active_experiment_needs_sample"):
        active_due = int(status.get("active_experiment_new_due_count") or 0)
        active_sends = int(status.get("active_experiment_sample_sends") or status.get("active_experiment_sends") or 0)
        active_experiment = status.get("active_experiment") if isinstance(status.get("active_experiment"), dict) else None
        target = int(status.get("active_experiment_sample_target") or _experiment_sample_target(active_experiment))
        variant = str(status.get("active_experiment_variant") or "active experiment")
        if active_due > 0:
            if status.get("send_window_is_open"):
                return f"Build {variant} sample now: send fresh first-touch leads before old follow-ups."
            return f"{variant} needs sample {active_sends}/{target}; {_send_window_wait_text(status)}."
        generic_due = int(status.get("active_experiment_generic_new_due_count") or 0)
        generic_allowed = int(status.get("active_experiment_allowed_generic_new_due_count") or 0)
        if generic_due > 0 and generic_allowed == 0:
            return (
                f"{variant} has {generic_due} generic first-touch leads, but the generic fallback cap is 0. "
                "Refill named direct buyers before using more volume."
            )
        direct_due = int(status.get("direct_due_count") or 0)
        if direct_due > 0 and int(status.get("cap_remaining") or 0) > 0:
            if status.get("send_window_is_open"):
                return (
                    f"{variant} needs sample {active_sends}/{target}, but no fresh first-touch leads are ready. "
                    "Send ready direct follow-ups while refilling fresh named buyers."
                )
            return (
                f"{variant} needs sample {active_sends}/{target}, but no fresh first-touch leads are ready. "
                f"Direct follow-ups are ready; {_send_window_wait_text(status)} while refilling fresh named buyers."
            )
        return f"Refill fresh direct buyer leads for {variant}; old follow-ups do not count toward the new experiment."
    if int(status.get("sent_today") or 0) >= int(status.get("daily_send_cap") or 0) and int(status.get("replies_today") or 0) == 0:
        return "Cap is used with no replies today; do not scale volume until targeting/copy produces a reply signal."
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
    if int(status.get("direct_due_count") or 0) > 0 and int(status.get("cap_remaining") or 0) > 0:
        if status.get("send_window_is_open"):
            return "Send direct decision-maker leads now; keep generic inboxes paused."
        return f"Direct decision-maker leads are ready; {_send_window_wait_text(status)}."
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
    active_experiment = status.get("active_experiment") if isinstance(status.get("active_experiment"), dict) else None
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
        "active_experiment_variant": str(status.get("active_experiment_variant") or ""),
        "active_experiment_sends": int(status.get("active_experiment_sends") or 0),
        "active_experiment_sample_sends": int(
            status.get("active_experiment_sample_sends") or status.get("active_experiment_sends") or 0
        ),
        "active_experiment_sample_sends_observed": int(
            status.get("active_experiment_sample_sends_observed")
            or status.get("active_experiment_sample_sends")
            or status.get("active_experiment_sends")
            or 0
        ),
        "active_experiment_sample_target": _experiment_sample_target(active_experiment),
        "active_experiment_needs_sample": bool(status.get("active_experiment_needs_sample") or False),
        "active_experiment_new_due_count": int(status.get("active_experiment_new_due_count") or 0),
        "active_experiment_blocked_after_sample_complete_count": int(
            status.get("active_experiment_blocked_after_sample_complete_count") or 0
        ),
        "active_experiment_direct_new_due_count": int(status.get("active_experiment_direct_new_due_count") or 0),
        "active_experiment_generic_new_due_count": int(status.get("active_experiment_generic_new_due_count") or 0),
        "active_experiment_allowed_generic_new_due_count": int(status.get("active_experiment_allowed_generic_new_due_count") or 0),
        "active_experiment_generic_sample_daily_cap": _active_generic_sample_cap(),
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
    status["autonomous_mode"] = (
        "refill direct buyer leads, send only inside the cold-send window, "
        "poll replies, and close through the notes-first test path"
    )
    status["notes_intake_url"] = _notes_url()
    status["paid_packet_url_configured"] = bool(settings.packet_checkout_url)
    status["next_money_move"] = _next_money_move(status)
    return status


def optimized_send_due_sequence_messages(limit: int | None = None) -> dict[str, Any]:
    import app.services.custom_outreach as outreach

    active_experiment = _active_experiment()
    active_variant = str(active_experiment.get("experiment_variant") or "control_sample_ask")
    effective_daily_cap = _effective_daily_cap(active_experiment)
    limit = min(limit or effective_daily_cap, effective_daily_cap)
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
            "effective_daily_cap": effective_daily_cap,
            "active_experiment_variant": active_variant,
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

        direct_due: list[tuple[AcquisitionProspect, Any, str, bool]] = []
        generic_due: list[tuple[AcquisitionProspect, Any, str, bool]] = []
        reserved_emails: set[str] = set()
        blocked_bad_email = 0
        duplicate_email_blocked = 0
        weak_decision_maker_blocked = 0
        active_variant_sends = _active_variant_send_count(session, active_variant, active_experiment)
        active_variant_sample_sends_observed = _active_variant_sample_send_count(
            session,
            active_variant,
            active_experiment,
        )
        experiment_sample_target = _experiment_sample_target(active_experiment)
        active_variant_sample_sends = min(active_variant_sample_sends_observed, experiment_sample_target)
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
            is_active_experiment_new = prospect_variant == active_variant and not sent_events and step.step_number == 1
            if _is_generic_inbox(prospect.contact_email):
                generic_due.append((prospect, step, prospect_variant, is_active_experiment_new))
            else:
                direct_due.append((prospect, step, prospect_variant, is_active_experiment_new))
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

        active_sample_needed = max(experiment_sample_target - active_variant_sample_sends, 0)
        active_sample_complete = active_sample_needed <= 0
        active_first_touch_candidates = [candidate for candidate in candidates if candidate[3]]
        active_variant_candidates = [candidate for candidate in candidates if candidate[2] == active_variant]
        active_variant_paused_after_sample_complete = 0
        if active_sample_complete and active_variant_candidates:
            active_variant_paused_after_sample_complete = len(active_variant_candidates)
            candidates = [candidate for candidate in candidates if candidate[2] != active_variant]
        active_direct_first_touch = [candidate for candidate in direct_due if candidate[3]]
        active_generic_first_touch = [candidate for candidate in generic_due if candidate[3]]
        generic_sample_cap = _active_generic_sample_cap()
        generic_active_sample_fallback_count = 0
        money_fill_after_active_sample_count = 0
        fill_remaining_after_active_sample = _fill_remaining_cap_after_active_sample()
        if active_sample_needed > 0 and (active_direct_first_touch or active_generic_first_touch):
            generic_slots = max(min(active_sample_needed - len(active_direct_first_touch), generic_sample_cap), 0)
            generic_active_sample_fallback = active_generic_first_touch[:generic_slots]
            generic_active_sample_fallback_count = len(generic_active_sample_fallback)
            active_sample_candidates = (active_direct_first_touch + generic_active_sample_fallback)[:active_sample_needed]
            active_sample_ids = {candidate[0].external_id for candidate in active_sample_candidates}
            money_fill_candidates = (
                [
                    candidate
                    for candidate in direct_due
                    if candidate[0].external_id not in active_sample_ids and candidate[2] != active_variant
                ]
                if fill_remaining_after_active_sample
                else []
            )
            money_fill_after_active_sample_count = len(money_fill_candidates)
            candidates = active_sample_candidates + money_fill_candidates
            active_sample_reserved_only = True
        else:
            active_sample_reserved_only = False

        remaining_cap = max(effective_daily_cap - outreach._daily_send_count(session), 0)

        for prospect, step, candidate_variant, is_active_experiment_new in candidates:
            if sent >= limit or remaining_cap <= 0:
                break
            if active_sample_complete and candidate_variant == active_variant:
                skipped += 1
                continue

            try:
                sent_events = outreach._sent_events_for_prospect(session, prospect.external_id)
                prospect_variant = _prospect_variant(sent_events, candidate_variant or active_variant)
                if active_sample_complete and prospect_variant == active_variant:
                    skipped += 1
                    continue
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
                        "active_experiment_sample_target": experiment_sample_target,
                        "active_experiment_sends_before": active_variant_sends,
                        "active_experiment_sample_sends_before": active_variant_sample_sends,
                        "active_experiment_sample_sends_observed_before": active_variant_sample_sends_observed,
                        "active_experiment_first_touch": is_active_experiment_new,
                        "body_preview": outreach._preview_text(plain_text, limit=240),
                        "body_text": plain_text,
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
        "skipped_count": skipped + paused_generic + active_variant_paused_after_sample_complete,
        "failures": failures,
        "active_experiment": active_experiment,
        "effective_daily_cap": effective_daily_cap,
        "quality_gate": {
            "policy": policy,
            "direct_due_count": len(direct_due),
            "generic_due_count": len(generic_due),
            "generic_paused_count": paused_generic,
            "active_experiment_variant": active_variant,
            "active_experiment_sends_before": active_variant_sends,
            "active_experiment_sample_sends_before": active_variant_sample_sends,
            "active_experiment_sample_sends_observed_before": active_variant_sample_sends_observed,
            "active_experiment_sample_target": experiment_sample_target,
            "active_experiment_new_due_count": 0
            if active_sample_complete
            else len(active_direct_first_touch) + generic_active_sample_fallback_count,
            "active_experiment_direct_new_due_count": len(active_direct_first_touch),
            "active_experiment_generic_new_due_count": len(active_generic_first_touch),
            "active_experiment_allowed_generic_new_due_count": generic_active_sample_fallback_count,
            "active_experiment_generic_sample_daily_cap": generic_sample_cap,
            "active_generic_sample_fallback_count": generic_active_sample_fallback_count,
            "active_first_touch_blocked_after_sample_complete": len(active_first_touch_candidates)
            if active_sample_complete
            else 0,
            "active_variant_paused_after_sample_complete": active_variant_paused_after_sample_complete,
            "active_sample_reserved_only": active_sample_reserved_only,
            "fill_remaining_cap_after_active_sample": fill_remaining_after_active_sample,
            "money_fill_after_active_sample_count": money_fill_after_active_sample_count,
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
    try:
        from app.services.post_purchase_autopilot import run_inbound_conversion_sweep

        inbound_conversion_result = run_inbound_conversion_sweep()
    except Exception as exc:
        inbound_conversion_result = {"status": "error", "summary": str(exc)}
    return {
        "send_result": send_result,
        "reply_result": reply_result,
        "inbound_conversion_result": inbound_conversion_result,
        "status": outreach.outreach_status(),
    }


def _zero_touch_close_reply() -> str:
    return (
        "Yes - here is the sample packet:\n"
        f"{_sample_url()}\n\n"
        "The easiest next step is to send one messy call note here:\n"
        f"{_notes_url()}\n\n"
        "If you want the paid priority test now, the one-call packet is $40:\n"
        f"{settings.packet_checkout_url}\n\n"
        "I will turn it into the recap, follow-up draft, open questions, and CRM-ready update.\n\n"
        "- Alan"
    )


def optimized_auto_reply_text(reply_text: str) -> tuple[str, str | None]:
    try:
        from app.services.hot_reply_closer import build_hot_reply_decision

        decision = build_hot_reply_decision(reply_text)
        if decision.intent == "negative":
            return "negative", None
        if decision.reply_text:
            return decision.intent, decision.reply_text
    except Exception:
        pass

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


def _apollo_email(person: dict[str, Any]) -> str:
    return _normalized_email(
        str(
            person.get("email")
            or person.get("contact_email")
            or person.get("work_email")
            or person.get("business_email")
            or ""
        )
    )


def _apollo_org(person: dict[str, Any]) -> dict[str, Any]:
    organization = person.get("organization")
    return organization if isinstance(organization, dict) else {}


def _apollo_person_id(person: dict[str, Any]) -> str:
    return str(person.get("id") or person.get("person_id") or person.get("apollo_id") or "").strip()


def _apollo_person_key(person: dict[str, Any]) -> str:
    return (
        _apollo_person_id(person)
        or str(person.get("linkedin_url") or person.get("linkedin") or "").strip().lower()
        or _normalized_email(str(person.get("email") or person.get("contact_email") or ""))
        or str(person.get("name") or "").strip().lower()
    )


def _apollo_enrichment_detail(person: dict[str, Any]) -> dict[str, Any]:
    organization = _apollo_org(person)
    website = (
        organization.get("website_url")
        or person.get("website_url")
        or person.get("website")
        or person.get("domain")
        or ""
    )
    detail = {
        "id": _apollo_person_id(person),
        "first_name": str(person.get("first_name") or "").strip(),
        "last_name": str(person.get("last_name") or "").strip(),
        "name": str(person.get("name") or "").strip(),
        "linkedin_url": str(person.get("linkedin_url") or person.get("linkedin") or "").strip(),
        "domain": str(website or "").strip().lower().removeprefix("https://").removeprefix("http://").split("/")[0].removeprefix("www."),
        "organization_name": str(organization.get("name") or person.get("company_name") or "").strip(),
    }
    return {key: value for key, value in detail.items() if value}


def _apollo_locations(value: Any) -> list[str]:
    raw_values = value if isinstance(value, list) else [value]
    mapped: list[str] = []
    for item in raw_values:
        text = str(item or "").strip()
        if not text:
            continue
        if text.upper() in {"US", "USA", "UNITED STATES OF AMERICA"}:
            text = "United States"
        if text not in mapped:
            mapped.append(text)
    return mapped or ["United States"]


def _apollo_primary_person(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for key in ("person", "contact", "match"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def _apollo_bulk_people(payload: dict[str, Any]) -> list[dict[str, Any]]:
    containers: list[Any] = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        containers.append(data)
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("people", "contacts", "matches", "results"):
            value = container.get(key)
            if isinstance(value, list):
                return [_apollo_primary_person(item) for item in value if isinstance(_apollo_primary_person(item), dict)]
    person = _apollo_primary_person(payload)
    return [person] if person else []


def _merge_apollo_person(base: dict[str, Any], enriched: dict[str, Any]) -> dict[str, Any]:
    organization = _apollo_org(base) or _apollo_org(enriched)
    merged = {**base, **enriched}
    if organization:
        merged["organization"] = {**organization, **_apollo_org(enriched)}
    return merged


async def _enrich_apollo_rows_with_email(
    client: ApolloClient,
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    enriched_rows = [dict(row) for row in rows]
    missing = [row for row in enriched_rows if not _apollo_email(row)]
    stats: dict[str, Any] = {
        "enrich_attempted": len(missing),
        "enriched_with_email": 0,
        "enrich_errors": [],
    }
    if not missing:
        return enriched_rows, stats

    reveal_personal = os.getenv("APOLLO_REVEAL_PERSONAL_EMAILS", "").strip().lower() in {"1", "true", "yes"}
    run_waterfall = os.getenv("APOLLO_RUN_WATERFALL_EMAIL", "").strip().lower() in {"1", "true", "yes"}

    enriched_by_key: dict[str, dict[str, Any]] = {}
    for start in range(0, len(missing), 10):
        batch = missing[start : start + 10]
        details = [_apollo_enrichment_detail(row) for row in batch]
        detail_pairs = [(row, detail) for row, detail in zip(batch, details) if detail]
        if not detail_pairs:
            continue
        try:
            bulk = await client.bulk_enrich_people(
                [detail for _, detail in detail_pairs],
                reveal_personal_emails=reveal_personal,
                run_waterfall_email=run_waterfall,
            )
            people = _apollo_bulk_people(bulk)
        except Exception as exc:
            stats["enrich_errors"].append(type(exc).__name__)
            people = []
            for _, detail in detail_pairs:
                try:
                    people.append(_apollo_primary_person(await client.enrich_person(detail)))
                except Exception as single_exc:
                    stats["enrich_errors"].append(type(single_exc).__name__)

        keyed_people = {_apollo_person_key(person): person for person in people if _apollo_person_key(person)}
        for row, _detail in detail_pairs:
            key = _apollo_person_key(row)
            person = keyed_people.get(key)
            if person is None and people:
                person = people.pop(0)
            if person and _apollo_email(person):
                enriched_by_key[key or _apollo_person_key(person)] = person

    for index, row in enumerate(enriched_rows):
        if _apollo_email(row):
            continue
        key = _apollo_person_key(row)
        enriched = enriched_by_key.get(key)
        if enriched:
            merged = _merge_apollo_person(row, enriched)
            enriched_rows[index] = merged
            if _apollo_email(merged):
                stats["enriched_with_email"] += 1

    stats["enrich_errors"] = sorted(set(stats["enrich_errors"]))
    stats["missing_email_after_enrichment"] = sum(1 for row in enriched_rows if not _apollo_email(row))
    return enriched_rows, stats


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
    search_payload.setdefault(
        "organization_locations",
        _apollo_locations(payload.get("organization_locations") or [settings.default_country]),
    )
    search_payload.setdefault("contact_email_status", payload.get("contact_email_status") or ["verified"])
    if q_keywords:
        search_payload.setdefault("q_keywords", q_keywords)

    result = await client.search_people(search_payload)
    rows = [row for row in acq._extract_people_rows(result) if isinstance(row, dict)]
    rows, enrich_stats = await _enrich_apollo_rows_with_email(client, rows)

    with acq._session() as session:
        count = 0
        skipped_bad = 0
        skipped_generic = 0
        skipped_missing_email = 0
        for person in rows:
            organization = person.get("organization") or {}
            email = _apollo_email(person)
            if not email:
                skipped_missing_email += 1
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
        "enriched_with_email": enrich_stats.get("enriched_with_email", 0),
        "enrich_attempted": enrich_stats.get("enrich_attempted", 0),
        "missing_email_after_enrichment": enrich_stats.get("missing_email_after_enrichment", 0),
        "enrich_errors": enrich_stats.get("enrich_errors", []),
        "apollo_endpoint": result.get("_apollo_endpoint"),
        "apollo_primary_error_status": result.get("_apollo_primary_error_status"),
        "skipped_bad_emails": skipped_bad,
        "skipped_generic_inboxes": skipped_generic,
        "skipped_missing_email": skipped_missing_email,
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
    ops.run_custom_outreach_cycle = optimized_run_custom_outreach_cycle
    ops.outreach_status = optimized_outreach_status

    outreach.STEP_TEMPLATES = OPTIMIZED_STEP_TEMPLATES
    outreach.STEP_TEMPLATE_VARIANTS = {
        **(getattr(outreach, "STEP_TEMPLATE_VARIANTS", {}) or {}),
        **OPTIMIZED_STEP_TEMPLATE_VARIANTS,
    }
    outreach._landing_page_url = _landing_page_url
    outreach._sample_url = _sample_url
    outreach._notes_url = _notes_url
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
