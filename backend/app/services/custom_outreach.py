from __future__ import annotations

import email
import imaplib
import json
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Any, Dict

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.resend_client import ResendClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.services.acquisition_supervisor import _auto_reply_text


@dataclass
class StepTemplate:
    step_number: int
    subject: str
    body: str
    delay_after_prev_days: int


@dataclass
class SMTPMailbox:
    slot: int
    address: str
    password: str


LEGACY_STEP_TEMPLATES: list[StepTemplate] = [
    StepTemplate(
        step_number=1,
        subject="after-call follow-up",
        body=(
            "Hey —\n\n"
            "After a strong client call, do you ever end up with the recap / next steps / follow-up sitting longer than it should?\n\n"
            "I’m testing a small done-for-you service that takes rough call notes and turns them into something clean and usable the same day.\n\n"
            "Happy to send a sample if that’s relevant.\n\n"
            "- Alan"
        ),
        delay_after_prev_days=0,
    ),
    StepTemplate(
        step_number=2,
        subject="re: after-call follow-up",
        body=(
            "Just following up once.\n\n"
            "This is the kind of output I mean:\n"
            "{landing_page_url}\n\n"
            "It’s not software to set up. You send rough notes from one real call, and you get back the finished recap, next steps, follow-up draft, and CRM-ready update.\n\n"
            "Would that be useful on one real call?\n\n"
            "- Alan"
        ),
        delay_after_prev_days=1,
    ),
    StepTemplate(
        step_number=3,
        subject="re: after-call follow-up",
        body=(
            "Last nudge.\n\n"
            "The easiest way to test it is one real call:\n"
            "{packet_checkout_url}\n\n"
            "If the pain is recurring, the bigger options are here:\n"
            "{landing_page_url}\n\n"
            "- Alan"
        ),
        delay_after_prev_days=2,
    ),
]


STEP_TEMPLATES = [
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


STEP_TEMPLATE_VARIANTS: dict[str, list[StepTemplate]] = {
    "control_sample_ask": STEP_TEMPLATES,
    "sample_first_plain": [
        StepTemplate(
            step_number=1,
            subject="after-call follow-up",
            body=(
                "Hey - quick question.\n\n"
                "I built Relay to turn rough notes from one sales or client call into the finished recap, follow-up draft, next steps, and CRM-ready update.\n\n"
                "Here is the sample packet:\n"
                "{sample_url}\n\n"
                "If there is one messy follow-up from this week, worth testing the $40 version?\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: after-call follow-up",
            body=(
                "Following up once.\n\n"
                "The useful part is not more software. It is getting the post-call cleanup finished when the team is busy.\n\n"
                "If you send rough notes from one real call, Relay sends back the recap, follow-up draft, open questions, and CRM-ready update.\n\n"
                "Sample:\n"
                "{sample_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: after-call follow-up",
            body=(
                "Last note from me.\n\n"
                "If after-call cleanup is not a problem, no worries.\n\n"
                "If it is, the smallest test is one call for $40:\n"
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
                "I built Relay for that exact cleanup. Your team sends rough notes, and the finished handoff comes back ready to use.\n\n"
                "If that is already handled, ignore me. If not, worth sending the sample?\n\n"
                "- Alan"
            ),
            delay_after_prev_days=0,
        ),
        StepTemplate(
            step_number=2,
            subject="re: who owns after-call cleanup?",
            body=(
                "Following up once with the concrete version.\n\n"
                "Sample packet:\n"
                "{sample_url}\n\n"
                "This is built for the gap after a good call: the work is obvious, but nobody wants to turn rough notes into a clean client-ready handoff.\n\n"
                "Worth testing on one real call?\n\n"
                "- Alan"
            ),
            delay_after_prev_days=1,
        ),
        StepTemplate(
            step_number=3,
            subject="re: who owns after-call cleanup?",
            body=(
                "Last note from me.\n\n"
                "If post-call cleanup is not costing time, no worries.\n\n"
                "If one call is worth testing, the $40 version is here:\n"
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
                "Would one messy call from this week be worth trying?\n\n"
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
                "If it is relevant later, sample is here:\n"
                "{sample_url}\n\n"
                "- Alan"
            ),
            delay_after_prev_days=2,
        ),
    ],
}


def _active_experiment_context() -> dict[str, Any]:
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
    if variant not in STEP_TEMPLATE_VARIANTS:
        variant = "control_sample_ask"
    experiment["experiment_variant"] = variant
    return experiment


def _templates_for_variant(variant: str) -> list[StepTemplate]:
    return STEP_TEMPLATE_VARIANTS.get(variant) or STEP_TEMPLATE_VARIANTS["control_sample_ask"]


def _prospect_variant(sent_events: list[AcquisitionEvent], fallback_variant: str) -> str:
    for event in sent_events:
        payload = _safe_json_loads(event.payload_json)
        variant = str(payload.get("experiment_variant") or "").strip()
        if variant in STEP_TEMPLATE_VARIANTS:
            return variant
    return fallback_variant if fallback_variant in STEP_TEMPLATE_VARIANTS else "control_sample_ask"


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


def _session() -> Session:
    return SessionLocal()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_start() -> datetime:
    return _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)


def _send_tz() -> ZoneInfo:
    tz_name = os.getenv("COLD_SEND_TIMEZONE", "America/New_York").strip() or "America/New_York"
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("America/New_York")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _next_allowed_send_start(
    now_local: datetime,
    *,
    start_hour: int,
    start_minute: int,
    business_days_only: bool,
) -> datetime:
    candidate = now_local.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)

    while business_days_only and candidate.weekday() >= 5:
        candidate = candidate + timedelta(days=1)

    return candidate


def _send_window_status() -> dict[str, Any]:
    tz = _send_tz()
    now_local = datetime.now(tz)

    start_hour = int(os.getenv("COLD_SEND_START_HOUR", "9").strip() or "9")
    start_minute = int(os.getenv("COLD_SEND_START_MINUTE", "30").strip() or "30")
    end_hour = int(os.getenv("COLD_SEND_END_HOUR", "11").strip() or "11")
    end_minute = int(os.getenv("COLD_SEND_END_MINUTE", "30").strip() or "30")

    start_local = now_local.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    end_local = now_local.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    business_days_only = _bool_env("COLD_SEND_BUSINESS_DAYS_ONLY", True)
    is_business_day = now_local.weekday() < 5

    is_open = start_local <= now_local < end_local
    if business_days_only:
        is_open = is_open and is_business_day

    if is_open:
        next_open_local = now_local
        reason = "open"
    elif business_days_only and not is_business_day:
        next_open_local = _next_allowed_send_start(
            now_local,
            start_hour=start_hour,
            start_minute=start_minute,
            business_days_only=business_days_only,
        )
        reason = "weekend"
    elif now_local < start_local:
        next_open_local = start_local
        reason = "before_window"
    else:
        next_open_local = _next_allowed_send_start(
            now_local,
            start_hour=start_hour,
            start_minute=start_minute,
            business_days_only=business_days_only,
        )
        reason = "after_window"

    seconds_until_open = max(int((next_open_local - now_local).total_seconds()), 0)

    return {
        "timezone": str(tz),
        "now_local": now_local.isoformat(),
        "start_local": start_local.isoformat(),
        "end_local": end_local.isoformat(),
        "is_open": is_open,
        "business_days_only": business_days_only,
        "is_business_day": is_business_day,
        "next_open_local": next_open_local.isoformat(),
        "seconds_until_open": seconds_until_open,
        "reason": reason,
    }


def _in_send_window() -> bool:
    return bool(_send_window_status().get("is_open"))


def _packet_5_pack_url() -> str:
    return getattr(settings, "packet_5_pack_url", "") or settings.packet_checkout_url


def _weekly_sprint_url() -> str:
    return (
        os.getenv("WEEKLY_SPRINT_URL", "").strip()
        or getattr(settings, "weekly_sprint_url", "")
        or _packet_5_pack_url()
    )


def _monthly_autopilot_url() -> str:
    return getattr(settings, "monthly_autopilot_url", "") or settings.packet_checkout_url


def _landing_page_url() -> str:
    url = os.getenv("LANDING_PAGE_URL", "").strip() or getattr(settings, "landing_page_url", "").strip()
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
    return local.startswith(("info", "hello", "contact", "admin", "support", "sales", "media", "press"))


def _render_body(template: StepTemplate, prospect: AcquisitionProspect) -> str:
    company_name = (prospect.company_name or "your agency").strip()
    return template.body.format(
        company_name=company_name,
        packet_checkout_url=settings.packet_checkout_url,
        packet_5_pack_url=_packet_5_pack_url(),
        weekly_sprint_url=_weekly_sprint_url(),
        monthly_autopilot_url=_monthly_autopilot_url(),
        landing_page_url=_landing_page_url(),
        sample_url=_sample_url(),
    )


def _log_event(
    session: Session,
    event_type: str,
    prospect_external_id: str,
    summary: str,
    payload: Dict[str, Any] | None = None,
) -> None:
    session.add(
        AcquisitionEvent(
            event_type=event_type,
            prospect_external_id=prospect_external_id,
            summary=summary,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
        )
    )


def _sent_events_for_prospect(session: Session, external_id: str) -> list[AcquisitionEvent]:
    return list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.prospect_external_id == external_id)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .order_by(AcquisitionEvent.created_at.asc())
        ).scalars().all()
    )


def _has_any_reply(session: Session, external_id: str) -> bool:
    count = session.execute(
        select(func.count(AcquisitionEvent.id))
        .where(AcquisitionEvent.prospect_external_id == external_id)
        .where(
            AcquisitionEvent.event_type.in_(
                ["custom_outreach_reply_seen", "smartlead_reply"]
            )
        )
    ).scalar()
    return int(count or 0) > 0


def _step_due(
    prospect: AcquisitionProspect,
    sent_events: list[AcquisitionEvent],
    templates: list[StepTemplate] | None = None,
) -> StepTemplate | None:
    templates = templates or STEP_TEMPLATES
    if prospect.status in {"paid", "intake_received", "replied", "manual_review"}:
        return None

    if not sent_events:
        return templates[0]

    if len(sent_events) >= len(templates):
        return None

    next_step = templates[len(sent_events)]
    last_sent = sent_events[-1].created_at
    if last_sent is None:
        return next_step

    due_at = last_sent + timedelta(days=next_step.delay_after_prev_days)
    if _now_utc().replace(tzinfo=None) >= due_at.replace(tzinfo=None):
        return next_step
    return None


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value or ""


def _extract_text_from_message(msg: email.message.Message) -> str:
    if msg.is_multipart():
        parts = []
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition.lower():
                continue
            if content_type == "text/plain":
                try:
                    parts.append(
                        part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8",
                            errors="ignore",
                        )
                    )
                except Exception:
                    pass
        return "\n".join(parts).strip()

    try:
        return msg.get_payload(decode=True).decode(
            msg.get_content_charset() or "utf-8",
            errors="ignore",
        ).strip()
    except Exception:
        return ""


def _smtp_enabled() -> bool:
    provider = os.getenv("COLD_OUTBOUND_PROVIDER", "").strip().lower()
    enabled = os.getenv("COLD_SMTP_ENABLED", "").strip().lower() == "true"
    return enabled or provider == "smtp"


def _smtp_mailboxes() -> list[SMTPMailbox]:
    mailboxes: list[SMTPMailbox] = []
    for i in range(1, 21):
        address = os.getenv(f"COLD_SMTP_MAILBOX_{i}_ADDRESS", "").strip()
        password = os.getenv(f"COLD_SMTP_MAILBOX_{i}_PASSWORD", "").strip()
        if address and password:
            mailboxes.append(SMTPMailbox(slot=i, address=address, password=password))
    return mailboxes


def _smtp_daily_counts(session: Session) -> dict[str, int]:
    rows = list(
        session.execute(
            select(AcquisitionEvent.payload_json)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .where(AcquisitionEvent.created_at >= _today_start())
        ).scalars().all()
    )

    counts: dict[str, int] = {}
    for payload_json in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            payload = {}
        if str(payload.get("provider") or "").strip().lower() != "smtp":
            continue
        sender = str(payload.get("sender_address") or "").strip().lower()
        if sender:
            counts[sender] = counts.get(sender, 0) + 1
    return counts


def _smtp_total_sent_today(session: Session) -> int:
    return sum(_smtp_daily_counts(session).values())


def _per_mailbox_daily_cap() -> int:
    raw = os.getenv("COLD_SMTP_PER_MAILBOX_DAILY_CAP", "").strip()
    if raw:
        try:
            return max(int(raw), 1)
        except Exception:
            pass

    mailboxes = _smtp_mailboxes()
    total_cap = settings.buyer_acq_daily_send_cap
    if not mailboxes:
        return total_cap
    return max(total_cap // len(mailboxes), 1)


def _choose_smtp_mailbox(session: Session) -> SMTPMailbox:
    mailboxes = _smtp_mailboxes()
    if not mailboxes:
        raise RuntimeError("No SMTP mailboxes configured")

    counts = _smtp_daily_counts(session)
    per_cap = _per_mailbox_daily_cap()

    available = [m for m in mailboxes if counts.get(m.address.lower(), 0) < per_cap]
    if not available:
        raise RuntimeError(f"All SMTP mailboxes are at cap ({per_cap}/day each)")

    return min(available, key=lambda m: counts.get(m.address.lower(), 0))


def _smtp_send(to_email: str, subject: str, plain_text: str, html_body: str) -> dict[str, Any]:
    host = os.getenv("COLD_SMTP_HOST", "smtp.porkbun.com").strip()
    port = int(os.getenv("COLD_SMTP_PORT", "587").strip() or "587")
    security = os.getenv("COLD_SMTP_SECURITY", "starttls").strip().lower()
    reply_to = os.getenv("COLD_SMTP_REPLY_TO", settings.reply_to_email or "").strip()

    with _session() as session:
        mailbox = _choose_smtp_mailbox(session)

    message = EmailMessage()
    message["From"] = mailbox.address
    message["To"] = to_email
    message["Subject"] = subject
    if reply_to:
        message["Reply-To"] = reply_to
    message.set_content(plain_text)
    message.add_alternative(
        f"<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.6'>{html_body}</div>",
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
        "status": "sent",
    }


def _outbound_send(to_email: str, subject: str, plain_text: str, html_body: str) -> dict[str, Any]:
    if _smtp_enabled():
        return _smtp_send(
            to_email=to_email,
            subject=subject,
            plain_text=plain_text,
            html_body=html_body,
        )

    resend_client = ResendClient()
    result = resend_client.send_outbound_email(
        to_email=to_email,
        subject=subject,
        html=f"<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.6'>{html_body}</div>",
    )
    return {
        "provider": "resend",
        "status": "sent",
        "result": result,
    }


def _daily_send_count(session: Session) -> int:
    if _smtp_enabled():
        return _smtp_total_sent_today(session)

    count = session.execute(
        select(func.count(AcquisitionEvent.id))
        .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
        .where(AcquisitionEvent.created_at >= _today_start())
    ).scalar()
    return int(count or 0)


def _total_send_count(session: Session) -> int:
    count = session.execute(
        select(func.count(AcquisitionEvent.id))
        .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
    ).scalar()
    return int(count or 0)


def send_due_sequence_messages(limit: int | None = None) -> dict[str, Any]:
    experiment = _active_experiment_context()
    active_variant = str(experiment.get("experiment_variant") or "control_sample_ask")
    base_cap = max(int(settings.buyer_acq_daily_send_cap or 0), 1)
    try:
        experiment_cap = int(experiment.get("daily_cap_recommendation") or base_cap)
    except Exception:
        experiment_cap = base_cap
    effective_daily_cap = max(min(experiment_cap, base_cap), 1)
    limit = min(limit or effective_daily_cap, effective_daily_cap)
    sent = 0
    skipped = 0
    failures: list[dict[str, Any]] = []

    window = _send_window_status()
    if not window["is_open"]:
        return {
            "status": "ok",
            "summary": "outside_send_window",
            "sent_count": 0,
            "skipped_count": 0,
            "failures": [],
            "send_window": window,
            "experiment": experiment,
            "effective_daily_cap": effective_daily_cap,
        }

    with _session() as session:
        prospects = list(
            session.execute(
                select(AcquisitionProspect)
                .where(AcquisitionProspect.contact_email != "")
                .where(
                    AcquisitionProspect.status.in_(
                        ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
                    )
                )
                .order_by(AcquisitionProspect.created_at.asc())
            ).scalars().all()
        )
        prospects.sort(
            key=lambda prospect: (
                _is_generic_inbox(prospect.contact_email),
                prospect.created_at or datetime.min,
            )
        )

        remaining_cap = max(effective_daily_cap - _daily_send_count(session), 0)

        for prospect in prospects:
            if sent >= limit or remaining_cap <= 0:
                break

            if _has_any_reply(session, prospect.external_id):
                skipped += 1
                continue

            sent_events = _sent_events_for_prospect(session, prospect.external_id)
            prospect_variant = _prospect_variant(sent_events, active_variant)
            templates = _templates_for_variant(prospect_variant)
            step = _step_due(prospect, sent_events, templates)
            if step is None:
                skipped += 1
                continue

            try:
                plain_text = _render_body(step, prospect)
                html_body = plain_text.replace("\n", "<br>")
                subject = step.subject
                result = _outbound_send(
                    to_email=prospect.contact_email,
                    subject=subject,
                    plain_text=plain_text,
                    html_body=html_body,
                )
                _log_event(
                    session,
                    f"custom_outreach_sent_step_{step.step_number}",
                    prospect.external_id,
                    f"sent custom outreach step {step.step_number}",
                    {
                        "to_email": prospect.contact_email,
                        "subject": subject,
                        "step_number": step.step_number,
                        "company_name": prospect.company_name,
                        "contact_name": prospect.contact_name,
                        "body_preview": _preview_text(plain_text, limit=240),
                        "plain_text": plain_text,
                        "experiment_variant": prospect_variant,
                        "experiment_label": experiment.get("experiment_label", ""),
                        "experiment_source": experiment.get("source", ""),
                        **result,
                    },
                )
                prospect.status = "sent_custom"
                session.commit()
                sent += 1
                remaining_cap -= 1
            except Exception as exc:
                failures.append({"external_id": prospect.external_id, "error": str(exc)})
                _log_event(
                    session,
                    "custom_outreach_send_failed",
                    prospect.external_id,
                    "custom outreach send failed",
                    {
                        "error": str(exc),
                        "step_number": step.step_number,
                        "experiment_variant": prospect_variant,
                    },
                )
                session.commit()

    return {
        "status": "ok",
        "sent_count": sent,
        "skipped_count": skipped,
        "failures": failures,
        "experiment": experiment,
        "effective_daily_cap": effective_daily_cap,
    }



def poll_reply_mailbox(limit: int | None = None) -> dict[str, Any]:
    limit = limit or settings.buyer_acq_reply_poll_limit
    processed = 0
    auto_replied = 0
    ignored = 0
    failures: list[str] = []

    if not settings.buyer_acq_mailbox_address or not settings.buyer_acq_mailbox_password:
        return {"status": "skipped", "summary": "missing mailbox credentials"}

    try:
        mail = imaplib.IMAP4_SSL(settings.buyer_acq_imap_host, settings.buyer_acq_imap_port)
        mail.login(settings.buyer_acq_mailbox_address, settings.buyer_acq_mailbox_password)
        mail.select("INBOX")
        typ, data = mail.search(None, "UNSEEN")
        if typ != "OK":
            mail.logout()
            return {"status": "error", "summary": "imap search failed"}

        msg_ids = data[0].split()[-limit:]
        if not msg_ids:
            mail.logout()
            return {"status": "ok", "processed": 0, "auto_replied": 0, "ignored": 0, "failures": []}

        with _session() as session:
            for msg_id in msg_ids:
                try:
                    typ, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if typ != "OK":
                        failures.append(f"fetch_failed:{msg_id.decode()}")
                        continue

                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)

                    from_name, from_email = parseaddr(_decode_header_value(msg.get("From")))
                    subject = _decode_header_value(msg.get("Subject"))
                    body = _extract_text_from_message(msg)

                    if not from_email:
                        ignored += 1
                        continue

                    prospect = session.execute(
                        select(AcquisitionProspect).where(
                            AcquisitionProspect.contact_email == from_email.lower()
                        )
                    ).scalar_one_or_none()

                    if prospect is None:
                        ignored += 1
                        continue

                    _log_event(
                        session,
                        "custom_outreach_reply_seen",
                        prospect.external_id,
                        "reply seen in mailbox",
                        {
                            "from_email": from_email,
                            "from_name": from_name,
                            "subject": subject,
                            "body": body[:3000],
                        },
                    )

                    intent, auto_reply = _auto_reply_text(body)

                    if intent == "negative":
                        prospect.status = "replied"
                    elif auto_reply:
                        _outbound_send(
                            to_email=from_email,
                            subject=f"Re: {subject or 'Quick question'}",
                            plain_text=auto_reply,
                            html_body=auto_reply.replace("\n", "<br>"),
                        )
                        _log_event(
                            session,
                            "custom_outreach_auto_reply_sent",
                            prospect.external_id,
                            f"auto replied ({intent})",
                            {
                                "subject": subject,
                                "reply_text": auto_reply,
                                "to_email": from_email,
                                "intent": intent,
                            },
                        )
                        prospect.status = (
                            "interested"
                            if intent in {"pricing", "link_request", "interested"}
                            else "auto_replied"
                        )
                        auto_replied += 1
                    else:
                        prospect.status = "manual_review"

                    processed += 1
                except Exception as exc:
                    failures.append(str(exc))

            session.commit()

        mail.logout()
    except Exception as exc:
        return {"status": "error", "summary": str(exc)}

    return {
        "status": "ok",
        "processed": processed,
        "auto_replied": auto_replied,
        "ignored": ignored,
        "failures": failures,
    }


def _due_now_count(session: Session) -> int:
    experiment = _active_experiment_context()
    active_variant = str(experiment.get("experiment_variant") or "control_sample_ask")
    prospects = list(
        session.execute(
            select(AcquisitionProspect)
            .where(AcquisitionProspect.contact_email != "")
            .where(
                AcquisitionProspect.status.in_(
                    ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
                )
            )
            .order_by(AcquisitionProspect.created_at.asc())
        ).scalars().all()
    )

    due = 0
    for prospect in prospects:
        if _has_any_reply(session, prospect.external_id):
            continue
        sent_events = _sent_events_for_prospect(session, prospect.external_id)
        prospect_variant = _prospect_variant(sent_events, active_variant)
        if _step_due(prospect, sent_events, _templates_for_variant(prospect_variant)) is not None:
            due += 1

    return due



def _safe_json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _preview_text(value: str | None, limit: int = 180) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)].rstrip() + "…"


def _full_sent_body(payload: dict[str, Any], prospect: AcquisitionProspect | None) -> str:
    for key in ("plain_text", "body_text", "body"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value

    step_number = payload.get("step_number")
    if prospect is not None and step_number is not None:
        try:
            step_index = int(step_number) - 1
        except Exception:
            step_index = -1
        if 0 <= step_index < len(STEP_TEMPLATES):
            try:
                return _render_body(STEP_TEMPLATES[step_index], prospect)
            except Exception:
                pass

    return str(payload.get("body_preview") or "").strip()


def _recent_sent_events(session: Session, limit: int = 8) -> list[dict[str, Any]]:
    rows = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type.like("custom_outreach_sent_step_%"))
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
        ).scalars().all()
    )
    external_ids = [row.prospect_external_id for row in rows if row.prospect_external_id]
    prospect_map: dict[str, AcquisitionProspect] = {}
    if external_ids:
        prospect_map = {
            p.external_id: p
            for p in session.execute(
                select(AcquisitionProspect).where(AcquisitionProspect.external_id.in_(external_ids))
            ).scalars().all()
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _safe_json_loads(row.payload_json)
        prospect = prospect_map.get(row.prospect_external_id)
        body = _full_sent_body(payload, prospect)
        items.append(
            {
                "created_at": row.created_at.isoformat() if row.created_at else "",
                "event_type": row.event_type,
                "prospect_external_id": row.prospect_external_id,
                "company_name": str(payload.get("company_name") or getattr(prospect, "company_name", "") or "").strip(),
                "contact_name": str(payload.get("contact_name") or getattr(prospect, "contact_name", "") or "").strip(),
                "to_email": str(payload.get("to_email") or getattr(prospect, "contact_email", "") or "").strip(),
                "subject": str(payload.get("subject") or "").strip(),
                "step_number": payload.get("step_number"),
                "sender_address": str(payload.get("sender_address") or "").strip(),
                "provider": str(payload.get("provider") or "").strip(),
                "body_preview": _preview_text(
                    str(payload.get("body_preview") or body or "")
                ),
                "body_text": body,
            }
        )
    return items


def _recent_reply_events(session: Session, limit: int = 8) -> list[dict[str, Any]]:
    rows = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "custom_outreach_reply_seen")
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
        ).scalars().all()
    )
    external_ids = [row.prospect_external_id for row in rows if row.prospect_external_id]
    prospect_map: dict[str, AcquisitionProspect] = {}
    if external_ids:
        prospect_map = {
            p.external_id: p
            for p in session.execute(
                select(AcquisitionProspect).where(AcquisitionProspect.external_id.in_(external_ids))
            ).scalars().all()
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _safe_json_loads(row.payload_json)
        prospect = prospect_map.get(row.prospect_external_id)
        body = str(payload.get("body") or "").strip()
        items.append(
            {
                "created_at": row.created_at.isoformat() if row.created_at else "",
                "event_type": row.event_type,
                "prospect_external_id": row.prospect_external_id,
                "company_name": str(getattr(prospect, "company_name", "") or "").strip(),
                "contact_name": str(getattr(prospect, "contact_name", "") or "").strip(),
                "from_email": str(payload.get("from_email") or "").strip(),
                "from_name": str(payload.get("from_name") or "").strip(),
                "subject": str(payload.get("subject") or "").strip(),
                "body_preview": _preview_text(body),
                "body_text": body,
                "status": str(getattr(prospect, "status", "") or "").strip(),
            }
        )
    return items


def _recent_auto_reply_events(session: Session, limit: int = 8) -> list[dict[str, Any]]:
    rows = list(
        session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == "custom_outreach_auto_reply_sent")
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(limit)
        ).scalars().all()
    )
    external_ids = [row.prospect_external_id for row in rows if row.prospect_external_id]
    prospect_map: dict[str, AcquisitionProspect] = {}
    if external_ids:
        prospect_map = {
            p.external_id: p
            for p in session.execute(
                select(AcquisitionProspect).where(AcquisitionProspect.external_id.in_(external_ids))
            ).scalars().all()
        }

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _safe_json_loads(row.payload_json)
        prospect = prospect_map.get(row.prospect_external_id)
        body = str(payload.get("reply_text") or "").strip()
        items.append(
            {
                "created_at": row.created_at.isoformat() if row.created_at else "",
                "event_type": row.event_type,
                "prospect_external_id": row.prospect_external_id,
                "company_name": str(getattr(prospect, "company_name", "") or "").strip(),
                "contact_name": str(getattr(prospect, "contact_name", "") or "").strip(),
                "to_email": str(payload.get("to_email") or getattr(prospect, "contact_email", "") or "").strip(),
                "subject": str(payload.get("subject") or "").strip(),
                "intent": str(payload.get("intent") or "").strip(),
                "body_preview": _preview_text(body),
                "body_text": body,
            }
        )
    return items

def outreach_status() -> dict[str, Any]:
    experiment = _active_experiment_context()
    configured_daily_cap = max(int(settings.buyer_acq_daily_send_cap or 0), 1)
    try:
        experiment_cap = int(experiment.get("daily_cap_recommendation") or configured_daily_cap)
    except Exception:
        experiment_cap = configured_daily_cap
    effective_daily_cap = max(min(experiment_cap, configured_daily_cap), 1)
    with _session() as session:
        sent_today = _daily_send_count(session)
        total_sends_all_time = _total_send_count(session)
        replies_today = int(
            session.execute(
                select(func.count(AcquisitionEvent.id))
                .where(AcquisitionEvent.event_type.in_(["custom_outreach_reply_seen", "smartlead_reply"]))
                .where(AcquisitionEvent.created_at >= _today_start())
            ).scalar()
            or 0
        )

        in_sequence_count = int(
            session.execute(
                select(func.count(AcquisitionProspect.id))
                .where(AcquisitionProspect.contact_email != "")
                .where(
                    AcquisitionProspect.status.in_(
                        ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
                    )
                )
            ).scalar()
            or 0
        )
        active_emails = list(
            session.execute(
                select(AcquisitionProspect.contact_email)
                .where(AcquisitionProspect.contact_email != "")
                .where(
                    AcquisitionProspect.status.in_(
                        ["scored", "queued_to_sender", "sent_custom", "sent_to_smartlead"]
                    )
                )
            ).scalars().all()
        )
        generic_inbox_count = sum(1 for email_address in active_emails if _is_generic_inbox(email_address))
        direct_inbox_count = max(len(active_emails) - generic_inbox_count, 0)

        due_now_count = _due_now_count(session)
        sender_counts = _smtp_daily_counts(session) if _smtp_enabled() else {}
        recent_sent = _recent_sent_events(session)
        recent_replies = _recent_reply_events(session)
        recent_auto_replies = _recent_auto_reply_events(session)

    window = _send_window_status()

    return {
        "queued_count": due_now_count,
        "due_now_count": due_now_count,
        "in_sequence_count": in_sequence_count,
        "direct_inbox_count": direct_inbox_count,
        "generic_inbox_count": generic_inbox_count,
        "sent_today": sent_today,
        "total_sends_all_time": total_sends_all_time,
        "replies_today": replies_today,
        "daily_send_cap": effective_daily_cap,
        "configured_daily_send_cap": configured_daily_cap,
        "provider": "smtp" if _smtp_enabled() else "resend",
        "sender_counts_today": sender_counts,
        "smtp_mailboxes_configured": len(_smtp_mailboxes()) if _smtp_enabled() else 0,
        "per_mailbox_daily_cap": _per_mailbox_daily_cap() if _smtp_enabled() else None,
        "reply_mailbox_address": settings.buyer_acq_mailbox_address,
        "reply_to_email": os.getenv("COLD_SMTP_REPLY_TO", settings.reply_to_email or "").strip(),
        "recent_sent": recent_sent,
        "recent_replies": recent_replies,
        "recent_auto_replies": recent_auto_replies,
        "active_experiment": experiment,
        "send_window_timezone": window["timezone"],
        "send_window_now_local": window["now_local"],
        "send_window_start_local": window["start_local"],
        "send_window_end_local": window["end_local"],
        "send_window_is_open": window["is_open"],
        "send_window_business_days_only": window["business_days_only"],
        "send_window_is_business_day": window["is_business_day"],
        "send_window_next_open_local": window["next_open_local"],
        "send_window_seconds_until_open": window["seconds_until_open"],
        "send_window_reason": window["reason"],
    }


def send_test_email(to_email: str) -> dict[str, Any]:
    try:
        return _outbound_send(
            to_email=to_email,
            subject="Quick question",
            plain_text="Hey - quick test from the custom sender.\n\n- Alan",
            html_body="Hey - quick test from the custom sender.<br><br>- Alan",
        )
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def run_custom_outreach_cycle() -> dict[str, Any]:
    send_result = send_due_sequence_messages()
    reply_result = poll_reply_mailbox()
    return {
        "send_result": send_result,
        "reply_result": reply_result,
        "status": outreach_status(),
    }
