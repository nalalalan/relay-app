from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Callable, Iterable, Protocol
from urllib.parse import urlparse

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.core.config import entry_checkout_url, settings
from app.db.base import SessionLocal
from app.models.buyer_acquisition_v1 import BuyerAcquisitionMessage, BuyerAcquisitionProspect


class MailerProtocol(Protocol):
    def send_plain_text(
        self,
        to_email: str,
        subject: str,
        body: str,
        reply_to: str | None = None,
        in_reply_to: str | None = None,
        references: str | None = None,
    ) -> Any: ...


class ReaderProtocol(Protocol):
    def fetch_unseen(self, limit: int = 20) -> list[Any]: ...
    def mark_seen(self, uids: Iterable[str]) -> None: ...


@dataclass
class ImportResult:
    imported: int
    updated: int
    rejected: int
    rows: list[dict[str, Any]]


@dataclass
class SendBatchResult:
    selected: int
    sent: int
    skipped: int
    rows: list[dict[str, Any]]


@dataclass
class ReplyPollResult:
    fetched: int
    matched: int
    auto_replied: int
    suppressed: int
    rows: list[dict[str, Any]]


_AGENCY_HINTS = {
    "paid media",
    "performance marketing",
    "ppc",
    "google ads",
    "meta ads",
    "media buying",
    "facebook ads",
    "lead generation",
    "dental marketing",
    "roofing marketing",
    "growth marketing",
}

_FOUNDER_HINTS = {
    "founder-led",
    "founder led",
    "boutique",
    "owner",
    "founder",
    "small team",
    "managing partner",
}

_CALL_HINTS = {
    "discovery call",
    "discovery calls",
    "consult",
    "consults",
    "sales call",
    "calls",
    "follow-up",
    "follow up",
    "handoff",
    "proposal",
    "crm",
    "next step",
    "next steps",
    "lead quality",
}

_EXCLUDED_HINTS = {
    "transcription",
    "note taking software",
    "saas platform",
    "podcast agency",
    "course creator",
    "job board",
    "staffing firm",
    "call recording",
    "virtual assistant marketplace",
}

_INTERESTED_RE = re.compile(
    r"\b(yes|interested|curious|sounds good|let'?s do it|worth trying|show me|send it over|open to it|tell me more)\b",
    re.IGNORECASE,
)
_QUESTION_RE = re.compile(r"\b(price|cost|sample|turnaround|how does it work|how quickly|what do you need)\b", re.IGNORECASE)
_NOT_NOW_RE = re.compile(r"\b(not now|later|circle back|next month|next quarter|too early)\b", re.IGNORECASE)
_NO_RE = re.compile(r"\b(not interested|no thanks|unsubscribe|remove me|stop emailing)\b", re.IGNORECASE)

_EMAIL_RE = re.compile(r"\b([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\b", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


def _session_factory(sf: Callable[[], Session] | None = None) -> Callable[[], Session]:
    return sf or SessionLocal


def _clean_text(value: Any) -> str:
    return _WS_RE.sub(" ", str(value or "")).strip()


def _clean_email(value: Any) -> str:
    return _clean_text(value).lower()


def _canonical_website(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    netloc = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.rstrip("/")
    return f"https://{netloc}{path}" if path else f"https://{netloc}"


def _domain(value: Any) -> str:
    website = _canonical_website(value)
    if not website:
        return ""
    return urlparse(website).netloc.lower().removeprefix("www.")


def _extract_emails_from_text(*chunks: Any) -> list[str]:
    found: list[str] = []
    for chunk in chunks:
        for email in _EMAIL_RE.findall(str(chunk or "")):
            cleaned = email.lower().strip().rstrip(".,;:")
            if cleaned not in found:
                found.append(cleaned)
    return found


def _prefer_contact_email(emails: list[str], domain: str) -> str:
    if not emails:
        return ""
    prioritized_prefixes = ["founder@", "owner@", "hello@", "alan@", "contact@", "info@"]
    for prefix in prioritized_prefixes:
        for email in emails:
            if email.startswith(prefix):
                return email
    for email in emails:
        if domain and email.endswith(f"@{domain}"):
            return email
    return emails[0]


def _score_prospect(company_name: str, website_text: str, notes: str) -> tuple[int, str, str, str]:
    hay = " ".join([company_name, website_text, notes]).lower()
    score = 0
    reasons: list[str] = []

    agency_hits = [token for token in _AGENCY_HINTS if token in hay]
    if agency_hits:
        score += 40
        reasons.append("looks like a paid media / performance agency")

    founder_hits = [token for token in _FOUNDER_HINTS if token in hay]
    if founder_hits:
        score += 15
        reasons.append("looks founder-led or boutique")

    call_hits = [token for token in _CALL_HINTS if token in hay]
    if call_hits:
        score += 20
        reasons.append("site language suggests real call follow-up friction")

    excluded_hits = [token for token in _EXCLUDED_HINTS if token in hay]
    if excluded_hits:
        score -= 50
        reasons.append("looks like a bad fit for the post-call packet offer")

    if score >= 70:
        fit_status = "strong"
    elif score >= 55:
        fit_status = "good"
    elif score >= 40:
        fit_status = "maybe"
    else:
        fit_status = "reject"

    personalization_line = _build_personalization_line(website_text, company_name)
    summary = "; ".join(reasons) if reasons else "thin public signal"
    return score, fit_status, summary, personalization_line


def _build_personalization_line(website_text: str, company_name: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", website_text or "")
    keywords = ("google ads", "meta ads", "paid media", "performance", "consult", "lead")
    for sentence in sentences:
        text = _clean_text(sentence)
        low = text.lower()
        if text and any(keyword in low for keyword in keywords):
            return text[:220]
    if company_name:
        return f"Saw {company_name} looks like a founder-led shop still turning discovery calls into manual cleanup work."
    return "Saw enough signal to think the post-call packet offer is relevant here."


def _prospect_id(company_name: str, website: str, source_query: str, source_name: str) -> str:
    seed = "|".join([_clean_text(company_name).lower(), _domain(website), _clean_text(source_query).lower(), _clean_text(source_name).lower()])
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
    return f"pros-{digest}"


def merge_source_and_crawl_records(source_records: list[dict[str, Any]], crawl_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_domain: dict[str, dict[str, Any]] = {}
    for row in source_records:
        merged = dict(row)
        merged["website"] = _canonical_website(row.get("website") or row.get("site") or row.get("url"))
        merged_domain = _domain(merged.get("website"))
        if merged_domain:
            by_domain[merged_domain] = merged
    for crawl in crawl_records:
        crawl_url = _canonical_website(crawl.get("url") or crawl.get("website") or crawl.get("site") or crawl.get("loadedUrl"))
        crawl_domain = _domain(crawl_url)
        if not crawl_domain or crawl_domain not in by_domain:
            continue
        website_text = _clean_text(crawl.get("text") or crawl.get("markdown") or crawl.get("content") or crawl.get("body"))
        existing = by_domain[crawl_domain]
        existing["website_text"] = " ".join(x for x in [existing.get("website_text", ""), website_text] if x).strip()
        existing["crawl_payload"] = crawl
    return list(by_domain.values())


def normalize_source_record(record: dict[str, Any]) -> dict[str, Any]:
    company_name = _clean_text(record.get("company_name") or record.get("title") or record.get("name"))
    website = _canonical_website(record.get("website") or record.get("site") or record.get("url") or record.get("domain"))
    domain = _domain(website)
    website_text = _clean_text(record.get("website_text") or record.get("text") or record.get("markdown") or record.get("description"))
    source_name = _clean_text(record.get("source_name") or record.get("source") or "apify")
    source_query = _clean_text(record.get("source_query") or record.get("query") or record.get("search_term"))
    city = _clean_text(record.get("city") or record.get("addressLocality") or record.get("locality"))
    region = _clean_text(record.get("region") or record.get("state") or record.get("addressRegion"))
    phone = _clean_text(record.get("phone") or record.get("phoneNumber") or record.get("phone_number"))
    notes = _clean_text(record.get("notes") or record.get("subtitle") or record.get("categoryName") or record.get("primary_category"))
    contact_name = _clean_text(record.get("contact_name") or record.get("owner") or record.get("founder"))
    contact_role = _clean_text(record.get("contact_role") or record.get("title") or record.get("role"))

    emails = _extract_emails_from_text(
        record.get("contact_email"),
        record.get("email"),
        record.get("emails"),
        website_text,
        json.dumps(record.get("crawl_payload") or {}, ensure_ascii=False),
    )
    contact_email = _prefer_contact_email(emails, domain)
    contact_source = "website_extract" if contact_email and _clean_email(record.get("contact_email") or record.get("email")) != contact_email else ("record" if contact_email else "")

    fit_score, fit_status, fit_summary, personalization_line = _score_prospect(company_name, website_text, notes)

    return {
        "external_id": _prospect_id(company_name, website, source_query, source_name),
        "company_name": company_name,
        "website": website,
        "domain": domain,
        "city": city,
        "region": region,
        "phone": phone,
        "source_name": source_name,
        "source_query": source_query,
        "contact_name": contact_name,
        "contact_email": contact_email,
        "contact_role": contact_role,
        "contact_source": contact_source,
        "website_text": website_text,
        "personalization_line": personalization_line,
        "fit_score": fit_score,
        "fit_status": fit_status,
        "fit_summary": fit_summary,
        "send_status": "new" if fit_status in {"strong", "good"} and contact_email else ("missing_email" if fit_status in {"strong", "good"} else "rejected"),
        "reply_status": "none",
        "suppression_reason": "",
        "notes": notes,
        "payload_json": json.dumps(record, ensure_ascii=False),
    }


def import_source_records(
    records: list[dict[str, Any]],
    session_factory: Callable[[], Session] | None = None,
) -> ImportResult:
    sf = _session_factory(session_factory)
    imported = 0
    updated = 0
    rejected = 0
    rows: list[dict[str, Any]] = []

    with sf() as session:
        for raw in records:
            normalized = normalize_source_record(raw)
            stmt = select(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.external_id == normalized["external_id"])
            prospect = session.execute(stmt).scalar_one_or_none()
            existed = prospect is not None
            if prospect is None:
                prospect = BuyerAcquisitionProspect(external_id=normalized["external_id"])
                session.add(prospect)
                session.flush()
                imported += 1
            else:
                updated += 1

            for key, value in normalized.items():
                if key == "external_id":
                    continue
                setattr(prospect, key, value)

            if normalized["fit_status"] == "reject":
                rejected += 1

            rows.append(
                {
                    "external_id": prospect.external_id,
                    "company_name": prospect.company_name,
                    "fit_status": prospect.fit_status,
                    "fit_score": prospect.fit_score,
                    "contact_email": prospect.contact_email,
                    "send_status": prospect.send_status,
                    "action": "updated" if existed else "imported",
                }
            )

        session.commit()

    return ImportResult(imported=imported, updated=updated, rejected=rejected, rows=rows)


def _build_initial_subject(prospect: BuyerAcquisitionProspect) -> str:
    base = prospect.company_name or prospect.domain or "your agency"
    return f"quick idea for {base} after discovery calls"


def _build_initial_body(prospect: BuyerAcquisitionProspect) -> str:
    opener = prospect.personalization_line or f"Saw {prospect.company_name} and thought this might fit."
    return (
        f"Hey{f' {prospect.contact_name.split()[0]}' if prospect.contact_name else ''},\n\n"
        f"{opener}\n\n"
        "I built a tiny done-for-you post-call packet for founder-led paid media shops. "
        "You drop in rough notes from one real discovery call and it sends back a client-ready recap, clear next steps, CRM-ready update text, and proposal-starting direction.\n\n"
        "Not trying to send you to a big funnel first. I just want to see if this is useful enough to run once on a real call.\n\n"
        "If you want, reply with yes and I’ll send the one-call path.\n\n"
        "- Alan"
    )


def send_initial_batch(
    session_factory: Callable[[], Session] | None = None,
    mailer: MailerProtocol | None = None,
    daily_cap: int | None = None,
    actually_send: bool = True,
) -> SendBatchResult:
    sf = _session_factory(session_factory)
    cap = int(daily_cap or settings.buyer_acq_daily_send_cap)
    rows: list[dict[str, Any]] = []
    sent = 0
    skipped = 0

    if mailer is None and actually_send:
        raise ValueError("mailer is required when actually_send=True")

    with sf() as session:
        stmt = (
            select(BuyerAcquisitionProspect)
            .where(BuyerAcquisitionProspect.send_status.in_(["new", "send_failed"]))
            .where(BuyerAcquisitionProspect.contact_email != "")
            .where(BuyerAcquisitionProspect.fit_status.in_(["strong", "good"]))
            .order_by(BuyerAcquisitionProspect.fit_score.desc(), BuyerAcquisitionProspect.created_at.asc())
            .limit(cap)
        )
        prospects = list(session.execute(stmt).scalars().all())

        for prospect in prospects:
            subject = _build_initial_subject(prospect)
            body = _build_initial_body(prospect)
            status = "queued"
            provider_message_id = ""
            if actually_send:
                result = mailer.send_plain_text(
                    to_email=prospect.contact_email,
                    subject=subject,
                    body=body,
                    reply_to=settings.reply_to_email or settings.buyer_acq_mailbox_address,
                )
                provider_message_id = str(getattr(result, "message_id", "") or "")
                status = "sent"
                sent += 1
            else:
                skipped += 1

            message = BuyerAcquisitionMessage(
                external_id=f"out-{prospect.external_id}-{int(datetime.utcnow().timestamp())}",
                prospect_external_id=prospect.external_id,
                direction="outbound",
                mailbox=settings.buyer_acq_mailbox_address,
                provider="porkbun_smtp" if actually_send else "dry_run",
                provider_message_id=provider_message_id,
                subject=subject,
                body_text=body,
                from_email=settings.buyer_acq_mailbox_address,
                to_email=prospect.contact_email,
                classification="initial_outreach",
                reply_action="await_reply",
                status=status,
                payload_json=json.dumps({"fit_status": prospect.fit_status, "fit_score": prospect.fit_score}, ensure_ascii=False),
            )
            session.add(message)
            prospect.send_status = status
            prospect.last_outbound_at = datetime.utcnow()
            rows.append(
                {
                    "prospect_external_id": prospect.external_id,
                    "company_name": prospect.company_name,
                    "to_email": prospect.contact_email,
                    "status": status,
                    "fit_score": prospect.fit_score,
                }
            )

        session.commit()

    return SendBatchResult(selected=len(rows), sent=sent, skipped=skipped, rows=rows)


def classify_reply_text(reply_text: str) -> str:
    text = _clean_text(reply_text).lower()
    if not text:
        return "unclear"
    if _NO_RE.search(text):
        return "no"
    if _NOT_NOW_RE.search(text):
        return "not_now"
    if _INTERESTED_RE.search(text):
        return "interested"
    if _QUESTION_RE.search(text):
        return "question"
    return "unclear"


def _build_positive_reply_body(prospect: BuyerAcquisitionProspect) -> str:
    intake_url = getattr(settings, "client_intake_url", "") or settings.client_intake_destination
    return (
        f"Great.\n\n"
        f"Fastest path is one real call. Pay here: {entry_checkout_url()}\n\n"
        f"Then drop the rough notes here: {intake_url}\n\n"
        "You’ll get back the packet by email with the recap, next steps, open questions, CRM-ready update text, and proposal-starting direction.\n\n"
        "- Alan"
    )


def _find_prospect_for_reply(session: Session, from_email: str) -> BuyerAcquisitionProspect | None:
    normalized = _clean_email(from_email)
    if not normalized:
        return None
    stmt = select(BuyerAcquisitionProspect).where(func.lower(BuyerAcquisitionProspect.contact_email) == normalized)
    prospect = session.execute(stmt).scalar_one_or_none()
    if prospect is not None:
        return prospect
    domain = normalized.split("@", 1)[-1] if "@" in normalized else ""
    if not domain:
        return None
    stmt = select(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.domain == domain).order_by(BuyerAcquisitionProspect.fit_score.desc())
    return session.execute(stmt).scalars().first()


def poll_and_route_replies(
    session_factory: Callable[[], Session] | None = None,
    reader: ReaderProtocol | None = None,
    mailer: MailerProtocol | None = None,
    limit: int | None = None,
    actually_send: bool = True,
    mark_seen: bool = True,
) -> ReplyPollResult:
    if reader is None:
        raise ValueError("reader is required")
    if mailer is None and actually_send:
        raise ValueError("mailer is required when actually_send=True")

    sf = _session_factory(session_factory)
    fetched_messages = reader.fetch_unseen(limit=int(limit or settings.buyer_acq_reply_poll_limit))
    matched = 0
    auto_replied = 0
    suppressed = 0
    processed_uids: list[str] = []
    rows: list[dict[str, Any]] = []

    with sf() as session:
        for item in fetched_messages:
            processed_uids.append(str(item.uid))
            existing_stmt = select(BuyerAcquisitionMessage).where(BuyerAcquisitionMessage.provider_message_id == str(item.message_id))
            if session.execute(existing_stmt).scalar_one_or_none() is not None:
                rows.append({"from_email": item.from_email, "status": "duplicate"})
                continue

            prospect = _find_prospect_for_reply(session, item.from_email)
            classification = classify_reply_text(item.body_text)
            message = BuyerAcquisitionMessage(
                external_id=f"in-{hashlib.sha1((item.message_id or item.uid).encode('utf-8')).hexdigest()[:12]}",
                prospect_external_id=prospect.external_id if prospect else "",
                direction="inbound",
                mailbox=settings.buyer_acq_mailbox_address,
                provider="porkbun_imap",
                provider_message_id=item.message_id,
                subject=item.subject,
                body_text=item.body_text,
                from_email=item.from_email,
                to_email=item.to_email,
                classification=classification,
                reply_action="",
                status="received",
                payload_json=json.dumps({"uid": item.uid, "raw_date": item.raw_date}, ensure_ascii=False),
            )
            session.add(message)

            if prospect is None:
                rows.append({"from_email": item.from_email, "status": "unmatched", "classification": classification})
                continue

            matched += 1
            prospect.last_inbound_at = datetime.utcnow()
            prospect.reply_status = classification

            if classification in {"interested", "question"}:
                response_subject = item.subject if item.subject.lower().startswith("re:") else f"Re: {item.subject}"
                response_body = _build_positive_reply_body(prospect)
                reply_status = "replied_positive"
                provider_message_id = ""
                if actually_send:
                    send_result = mailer.send_plain_text(
                        to_email=prospect.contact_email,
                        subject=response_subject,
                        body=response_body,
                        reply_to=settings.reply_to_email or settings.buyer_acq_mailbox_address,
                        in_reply_to=item.message_id or None,
                        references=item.message_id or None,
                    )
                    provider_message_id = str(getattr(send_result, "message_id", "") or "")
                    auto_replied += 1
                out_message = BuyerAcquisitionMessage(
                    external_id=f"reply-{prospect.external_id}-{int(datetime.utcnow().timestamp())}",
                    prospect_external_id=prospect.external_id,
                    direction="outbound",
                    mailbox=settings.buyer_acq_mailbox_address,
                    provider="porkbun_smtp" if actually_send else "dry_run",
                    provider_message_id=provider_message_id,
                    subject=response_subject,
                    body_text=response_body,
                    from_email=settings.buyer_acq_mailbox_address,
                    to_email=prospect.contact_email,
                    classification="offer_path",
                    reply_action="sent_payment_and_intake",
                    status="sent" if actually_send else "queued",
                    payload_json=json.dumps({"trigger_message_id": item.message_id}, ensure_ascii=False),
                )
                session.add(out_message)
                prospect.send_status = reply_status
                rows.append({"from_email": prospect.contact_email, "status": reply_status, "classification": classification})
            elif classification in {"no", "not_now"}:
                prospect.send_status = "suppressed"
                prospect.suppression_reason = classification
                suppressed += 1
                rows.append({"from_email": prospect.contact_email, "status": "suppressed", "classification": classification})
            else:
                prospect.send_status = "reply_needs_review"
                rows.append({"from_email": prospect.contact_email, "status": "needs_review", "classification": classification})

        session.commit()

    if mark_seen and processed_uids:
        reader.mark_seen(processed_uids)

    return ReplyPollResult(
        fetched=len(fetched_messages),
        matched=matched,
        auto_replied=auto_replied,
        suppressed=suppressed,
        rows=rows,
    )


def write_digest(target_path: str, session_factory: Callable[[], Session] | None = None) -> str:
    sf = _session_factory(session_factory)
    with sf() as session:
        total = session.execute(select(func.count()).select_from(BuyerAcquisitionProspect)).scalar_one()
        sent = session.execute(select(func.count()).select_from(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.send_status == "sent")).scalar_one()
        replied_positive = session.execute(select(func.count()).select_from(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.send_status == "replied_positive")).scalar_one()
        suppressed = session.execute(select(func.count()).select_from(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.send_status == "suppressed")).scalar_one()
        top_stmt = select(BuyerAcquisitionProspect).where(BuyerAcquisitionProspect.fit_status.in_(["strong", "good"]))\
            .order_by(BuyerAcquisitionProspect.fit_score.desc(), BuyerAcquisitionProspect.created_at.asc())\
            .limit(10)
        prospects = list(session.execute(top_stmt).scalars().all())
    lines = [
        "Buyer Acquisition Loop v1 digest",
        "",
        f"total_prospects: {total}",
        f"sent: {sent}",
        f"replied_positive: {replied_positive}",
        f"suppressed: {suppressed}",
        "",
        "Top prospects:",
    ]
    for prospect in prospects:
        lines.append(f"- {prospect.company_name} | fit={prospect.fit_score} | status={prospect.send_status} | email={prospect.contact_email}")
    with open(target_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    return target_path
