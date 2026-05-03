from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import httpx
from apify_client import ApifyClient
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.integrations.apollo import ApolloClient
from app.integrations.smartlead import SmartleadClient
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect


@dataclass
class AcquisitionTickResult:
    searched: int
    upserted: int
    enriched: int
    queued: int
    sent_live: int
    replied: int
    interested: int
    paid: int
    intake_received: int
    summary: str


EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

PUBLIC_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "pm.me",
}

PLATFORM_OR_PLACEHOLDER_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "email.com",
    "domain.com",
    "test.com",
    "yourdomain.com",
    "amazon.com",
    "facebook.com",
    "fb.com",
    "meta.com",
    "instagram.com",
    "linkedin.com",
    "google.com",
    "youtube.com",
    "tiktok.com",
    "wix.com",
    "wixpress.com",
    "shopify.com",
    "hubspot.com",
    "mailchimp.com",
    "canva.com",
    "adobe.com",
    "wordpress.com",
    "webflow.io",
    "squarespace.com",
}

BLOCKED_LOCAL_PARTS = {
    "user",
    "test",
    "example",
    "firstname",
    "lastname",
    "jane",
    "john",
    "yourname",
    "your-email",
    "your_email",
    "sample",
    "demo",
    "placeholder",
}

BLOCKED_EXACT_EMAILS = {
    "user@domain.com",
    "jane@email.com",
    "john@example.com",
    "example@example.com",
    "name@example.com",
    "test@test.com",
    "jeff@amazon.com",
}

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


def _session() -> Session:
    return SessionLocal()


def _clean_domain(raw: str) -> str:
    raw = (raw or "").strip().lower()
    raw = raw.removeprefix("https://").removeprefix("http://")
    raw = raw.split("/")[0]
    return raw.removeprefix("www.")


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _is_generic_inbox(email: str) -> bool:
    local = (email or "").split("@", 1)[0].strip().lower()
    if not local:
        return True
    local_base = local.replace(".", "").replace("-", "").replace("_", "")
    if local in GENERIC_INBOX_LOCAL_PARTS or local_base in GENERIC_INBOX_LOCAL_PARTS:
        return True
    return local.startswith(("info", "hello", "contact", "admin", "support", "sales"))


def _looks_fake_or_low_value_email(email: str, business_domain: str = "") -> bool:
    email = _normalize_email(email)
    if not email or "@" not in email:
        return True

    local, domain = email.split("@", 1)
    business_domain = _clean_domain(business_domain)

    if email in BLOCKED_EXACT_EMAILS:
        return True
    if local in BLOCKED_LOCAL_PARTS:
        return True
    if local.startswith("test") or local.endswith("example"):
        return True
    if domain in PLATFORM_OR_PLACEHOLDER_DOMAINS:
        return True

    if business_domain:
        if domain == business_domain or domain.endswith("." + business_domain):
            return False
        if domain in PUBLIC_EMAIL_DOMAINS:
            return False

    return False


def _extract_people_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("people", "contacts", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    if isinstance(payload.get("data"), dict):
        data = payload["data"]
        for key in ("people", "contacts", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def _get_company_name(row: Dict[str, Any]) -> str:
    org = row.get("organization") or {}
    return (
        row.get("company_name")
        or org.get("name")
        or row.get("account", {}).get("name", "")
        or ""
    ).strip()


def _get_website(row: Dict[str, Any]) -> str:
    org = row.get("organization") or {}
    return (
        row.get("website_url")
        or org.get("website_url")
        or row.get("website")
        or ""
    ).strip()


def _get_contact_name(row: Dict[str, Any]) -> str:
    direct = row.get("name")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    first = str(row.get("first_name", "")).strip()
    last = str(row.get("last_name", "")).strip()
    return (first + " " + last).strip()


def _score_prospect(title: str, company_name: str, website: str, notes: str) -> tuple[int, str, str]:
    score = 0
    hay = " ".join([title, company_name, website, notes]).lower()
    title_l = title.lower()

    founder_hits = ["founder", "co-founder", "owner", "managing partner", "ceo", "partner"]
    agency_hits = ["agency", "marketing", "media", "growth"]
    channel_hits = ["paid media", "ppc", "google ads", "meta ads", "performance marketing", "media buying", "search marketing"]

    noisy_or_wrong_fit_hits = [
        "amazon marketing",
        "amazon agency",
        "seo company",
        "lead generation",
        "real estate",
        "mortgage",
        "saas",
        "software",
        "call recording",
        "transcription",
        "virtual assistant",
    ]

    founder_match = any(hit in title_l for hit in founder_hits)
    agency_match = any(hit in hay for hit in agency_hits)
    channel_match = any(hit in hay for hit in channel_hits)

    if founder_match:
        score += 25
    if agency_match:
        score += 20
    if channel_match:
        score += 25

    if founder_match and agency_match and channel_match:
        score += 20

    if any(hit in hay for hit in noisy_or_wrong_fit_hits):
        score -= 45

    if channel_match and agency_match and score >= settings.acq_min_fit_score + 20:
        return score, "strong", "founder_paid_media_core"
    if channel_match and agency_match and score >= settings.acq_min_fit_score:
        return score, "good", "founder_paid_media_maybe"
    if channel_match and score >= 45:
        return score, "maybe", "borderline"
    return score, "trash", "reject"


def _initial_status(score: int, band: str, email: str) -> str:
    has_email = bool((email or "").strip())

    if band in {"strong", "good"}:
        return "scored"

    if band == "maybe" and has_email and score >= 45:
        return "scored"

    return "rejected"


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


def _upsert_prospect(session: Session, row: Dict[str, Any]) -> AcquisitionProspect:
    external_id = str(
        row.get("id")
        or row.get("person_id")
        or row.get("apollo_id")
        or row.get("email")
        or row.get("name")
        or f"prospect-{datetime.utcnow().timestamp()}"
    ).strip()

    stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == external_id)
    prospect = session.execute(stmt).scalar_one_or_none()
    if prospect is None:
        prospect = AcquisitionProspect(external_id=external_id)
        session.add(prospect)

    company_name = _get_company_name(row)
    website = _get_website(row)
    domain = _clean_domain(website or row.get("domain", ""))
    contact_name = _get_contact_name(row)
    title = str(row.get("title", "")).strip()
    notes = str(row.get("headline", "") or row.get("notes", "")).strip()
    contact_email = _normalize_email(str(row.get("email", "") or row.get("contact_email", "")))

    if _looks_fake_or_low_value_email(contact_email, business_domain=domain):
        contact_email = ""

    score, band, segment = _score_prospect(title, company_name, website, notes)

    prospect.company_name = company_name
    prospect.website = website
    prospect.domain = domain
    prospect.contact_name = contact_name
    prospect.contact_email = contact_email
    prospect.title = title
    prospect.source = str(row.get("source", "apollo")).strip() or "apollo"
    prospect.fit_score = score
    prospect.fit_band = band
    prospect.segment = segment
    prospect.notes = notes
    prospect.payload_json = json.dumps(row, ensure_ascii=False)

    if not prospect.status or prospect.status in {"new", "rejected"}:
        prospect.status = _initial_status(score, band, contact_email)

    _log_event(
        session,
        "prospect_upserted",
        prospect.external_id,
        f"prospect upserted with fit_band={band} fit_score={score} status={prospect.status}",
        row,
    )
    return prospect


async def _extract_email_from_website(website: str) -> str:
    if not website:
        return ""

    website = website.strip()
    business_domain = _clean_domain(website)

    if not website.startswith("http://") and not website.startswith("https://"):
        website = "https://" + website

    base = website.rstrip("/")
    candidates = [
        base,
        base + "/contact",
        base + "/contact-us",
        base + "/about",
        base + "/about-us",
        base + "/services",
    ]

    try:
        timeout_seconds = max(float(os.getenv("APIFY_EMAIL_EXTRACT_TIMEOUT_SECONDS", "4") or 4), 1.0)
    except Exception:
        timeout_seconds = 4.0

    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        async def fetch(url: str) -> str:
            try:
                resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                return resp.text or ""
            except Exception:
                return ""

        pages = await asyncio.gather(*(fetch(url) for url in candidates))
        for text in pages:
            matches = EMAIL_REGEX.findall(text)
            for email in matches:
                email_l = _normalize_email(email)
                if _looks_fake_or_low_value_email(email_l, business_domain=business_domain):
                    continue
                if any(
                    bad in email_l
                    for bad in [
                        ".png",
                        ".jpg",
                        ".jpeg",
                        ".webp",
                        ".svg",
                        "@sentry",
                        "@example",
                        "wixpress",
                        "cloudflare",
                    ]
                ):
                    continue
                return email_l

    return ""


async def import_from_apollo_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    client = ApifyClient(settings.apify_api_token)
    q_keywords = str(payload.get("q_keywords") or "ppc agency founder").strip() or "ppc agency founder"

    try:
        max_places = max(int(os.getenv("APIFY_MAX_CRAWLED_PLACES_PER_SEARCH", "20") or 20), 1)
    except Exception:
        max_places = 20

    run_input = {
        "searchStringsArray": [q_keywords],
        "locationQuery": "United States",
        "maxCrawledPlacesPerSearch": max_places,
    }

    run = client.actor(settings.apify_google_maps_actor_id).call(run_input=run_input)
    items = client.dataset(run["defaultDatasetId"]).list_items().items

    try:
        extract_concurrency = max(int(os.getenv("APIFY_EMAIL_EXTRACT_CONCURRENCY", "4") or 4), 1)
    except Exception:
        extract_concurrency = 4
    semaphore = asyncio.Semaphore(extract_concurrency)

    async def extract_email(item: Dict[str, Any]) -> str:
        async with semaphore:
            return await _extract_email_from_website(item.get("website") or "")

    emails = await asyncio.gather(*(extract_email(item) for item in items))

    with _session() as session:
        count = 0
        prospects_with_email = 0
        sendable_upserted = 0
        direct_sendable_upserted = 0
        generic_sendable_upserted = 0
        missing_email = 0
        rejected_or_unsendable = 0

        for item, email in zip(items, emails):
            website = item.get("website") or ""

            row = {
                "id": item.get("placeId") or item.get("title"),
                "name": item.get("title"),
                "company_name": item.get("title"),
                "website": website,
                "domain": website,
                "title": "Founder / Owner",
                "headline": " ".join([str(item.get("categoryName") or ""), q_keywords]).strip(),
                "email": email,
                "source": "apify",
            }

            prospect = _upsert_prospect(session, row)
            count += 1
            if prospect.contact_email:
                prospects_with_email += 1
            else:
                missing_email += 1
            if prospect.contact_email and prospect.status in {
                "scored",
                "queued_to_sender",
                "sent_custom",
                "sent_to_smartlead",
            }:
                sendable_upserted += 1
                if _is_generic_inbox(prospect.contact_email):
                    generic_sendable_upserted += 1
                else:
                    direct_sendable_upserted += 1
            else:
                rejected_or_unsendable += 1

        session.commit()

    return {
        "status": "ok",
        "source": "apify",
        "searched": len(items),
        "upserted": count,
        "prospects_with_email": prospects_with_email,
        "sendable_upserted": sendable_upserted,
        "direct_sendable_upserted": direct_sendable_upserted,
        "generic_sendable_upserted": generic_sendable_upserted,
        "missing_email_count": missing_email,
        "rejected_or_unsendable_count": rejected_or_unsendable,
    }


def _split_csv(value: str) -> list[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


async def import_from_apollo_people_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Import named decision-makers instead of storefront contact inboxes."""
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
    rows = _extract_people_rows(result)

    with _session() as session:
        count = 0
        for person in rows:
            organization = person.get("organization") or {}
            email = person.get("email") or person.get("contact_email") or ""
            if not email:
                continue

            row = {
                **person,
                "id": person.get("id") or person.get("person_id") or email,
                "person_id": person.get("id") or person.get("person_id") or "",
                "company_name": organization.get("name") or person.get("company_name") or "",
                "website": organization.get("website_url") or person.get("website_url") or person.get("website") or "",
                "title": person.get("title") or "",
                "headline": person.get("headline") or q_keywords,
                "email": email,
                "source": "apollo_people",
            }
            _upsert_prospect(session, row)
            count += 1

        session.commit()

    return {
        "status": "ok",
        "source": "apollo_people",
        "searched": len(rows),
        "upserted": count,
        "apollo_payload": {
            key: value
            for key, value in search_payload.items()
            if key not in {"api_key", "password", "token"}
        },
    }


async def enrich_unsent_prospects(limit: int = 10) -> int:
    with _session() as session:
        stmt = (
            select(AcquisitionProspect)
            .where(AcquisitionProspect.status == "scored")
            .where(AcquisitionProspect.contact_email == "")
            .order_by(AcquisitionProspect.created_at.asc())
            .limit(limit)
        )
        prospects = list(session.execute(stmt).scalars().all())

    enriched = 0
    client = ApolloClient()
    for prospect in prospects:
        payload = {
            "name": prospect.contact_name,
            "domain": prospect.domain,
            "organization_name": prospect.company_name,
        }
        try:
            result = await client.enrich_person(payload)
        except Exception:
            continue

        email = _normalize_email(
            result.get("person", {}).get("email")
            or result.get("email")
            or result.get("contact", {}).get("email")
            or ""
        )

        if email and not _looks_fake_or_low_value_email(email, business_domain=prospect.domain):
            with _session() as session:
                stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == prospect.external_id)
                current = session.execute(stmt).scalar_one()
                current.contact_email = email
                _log_event(session, "prospect_enriched", current.external_id, "email enriched from apollo", result)
                session.commit()
                enriched += 1

    return enriched


def _build_smartlead_payload(prospect: AcquisitionProspect) -> Dict[str, Any]:
    first_name = prospect.contact_name.split(" ")[0].strip() if prospect.contact_name else ""
    return {
        "email": prospect.contact_email,
        "first_name": first_name,
        "last_name": prospect.contact_name[len(first_name):].strip() if first_name else "",
        "company_name": prospect.company_name,
        "website": prospect.website,
        "custom_fields": {
            "fit_band": prospect.fit_band,
            "fit_score": str(prospect.fit_score),
            "segment": prospect.segment,
            "packet_offer_name": settings.packet_offer_name,
            "packet_checkout_url": settings.packet_checkout_url,
            "landing_page_url": settings.landing_page_url,
        },
    }


async def queue_best_prospects_to_smartlead(send_live: bool = False, limit: int = 25) -> Dict[str, int]:
    with _session() as session:
        stmt = (
            select(AcquisitionProspect)
            .where(AcquisitionProspect.status.in_(["scored", "queued_to_sender"]))
            .where(AcquisitionProspect.contact_email != "")
            .where(
                or_(
                    AcquisitionProspect.fit_band.in_(["strong", "good", "maybe"]),
                    AcquisitionProspect.fit_score >= 45,
                )
            )
            .order_by(AcquisitionProspect.fit_score.desc(), AcquisitionProspect.created_at.asc())
            .limit(limit)
        )
        prospects = list(session.execute(stmt).scalars().all())

    queued = 0
    sent_live = 0
    smartlead = SmartleadClient()

    for prospect in prospects:
        if _looks_fake_or_low_value_email(prospect.contact_email, business_domain=prospect.domain):
            with _session() as session:
                stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == prospect.external_id)
                current = session.execute(stmt).scalar_one()
                current.contact_email = ""
                current.status = "rejected"
                _log_event(session, "prospect_rejected", current.external_id, "fake or low-value email filtered before send")
                session.commit()
            continue

        if send_live and settings.smartlead_campaign_id and settings.smartlead_api_key:
            result = await smartlead.add_lead_to_campaign(
                settings.smartlead_campaign_id,
                _build_smartlead_payload(prospect),
            )
            smartlead_lead_id = str(result.get("id") or result.get("lead_id") or "")
            new_status = "sent_to_smartlead"
            sent_live += 1
        else:
            result = {"dry_run": True, "campaign_id": settings.smartlead_campaign_id}
            smartlead_lead_id = ""
            new_status = "queued_to_sender"

        with _session() as session:
            stmt = select(AcquisitionProspect).where(AcquisitionProspect.external_id == prospect.external_id)
            current = session.execute(stmt).scalar_one()
            current.smartlead_campaign_id = settings.smartlead_campaign_id
            if smartlead_lead_id:
                current.smartlead_lead_id = smartlead_lead_id
            current.status = new_status
            _log_event(session, "prospect_queued", current.external_id, f"prospect moved to {new_status}", result)
            session.commit()
            queued += 1

    return {"queued": queued, "sent_live": sent_live}


def _find_prospect_by_email(session: Session, email: str) -> AcquisitionProspect | None:
    stmt = select(AcquisitionProspect).where(AcquisitionProspect.contact_email == email)
    return session.execute(stmt).scalar_one_or_none()


def _reply_checkout_url() -> str:
    return (
        getattr(settings, "packet_checkout_url", "")
        or getattr(settings, "stripe_payment_link", "")
        or ""
    )


def _clean_reply_text(s: str) -> str:
    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u00a0": " ",
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)
    return s


def _zero_touch_reply() -> str:
    return _clean_reply_text(
        "totally - pick whichever is the best fit\n\n"
        f"one live packet ($40):\n{settings.packet_checkout_url}\n\n"
        f"5-call sprint ($750):\n{getattr(settings, 'packet_5_pack_url', '') or settings.packet_checkout_url}\n\n"
        f"done-for-you week ($3000):\n{getattr(settings, 'weekly_sprint_url', '') or settings.packet_checkout_url}\n\n"
        f"done-for-you month ($7500):\n{getattr(settings, 'monthly_autopilot_url', '') or settings.packet_checkout_url}\n\n"
        "after checkout, intake opens automatically"
    )

def _auto_reply_text(reply_text: str) -> tuple[str, str | None]:
    text = (reply_text or "").lower()

    if any(x in text for x in [
        "unsubscribe",
        "remove me",
        "stop",
        "not interested",
        "leave me alone",
        "go away",
    ]):
        return ("negative", None)

    if any(x in text for x in [
        "privacy",
        "security",
        "legal",
        "nda",
        "hipaa",
        "compliance",
        "refund",
        "guarantee",
        "contract",
        "enterprise",
        "custom",
        "wtf",
        "scam",
        "bot",
    ]):
        return ("manual_review", None)

    if any(x in text for x in [
        "send me the link",
        "send the link",
        "how do i start",
        "where do i start",
        "buy",
        "purchase",
        "checkout",
    ]):
        return ("link_request", _zero_touch_reply())

    if any(x in text for x in [
        "how much",
        "price",
        "pricing",
        "cost",
        "$",
        "40",
        "forty",
    ]):
        return ("pricing", _zero_touch_reply())

    if any(x in text for x in [
        "what is this",
        "how does it work",
        "tell me more",
        "what do i get",
        "what exactly",
        "packet",
    ]):
        return ("curious", _zero_touch_reply())

    if any(x in text for x in [
        "how fast",
        "turnaround",
        "when would i get it",
        "when can you deliver",
    ]):
        return ("speed", _zero_touch_reply())

    if any(x in text for x in [
        "interested",
        "sounds good",
        "let's do it",
        "lets do it",
        "i'm in",
        "im in",
        "worth trying",
        "can try",
        "maybe",
        "curious",
    ]):
        return ("interested", _zero_touch_reply())

    return ("zero_touch", _zero_touch_reply())


async def handle_smartlead_reply_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    email = str(
        payload.get("email")
        or payload.get("lead", {}).get("email")
        or payload.get("prospect", {}).get("email")
        or ""
    ).strip().lower()

    reply_text = str(
        payload.get("reply_text")
        or payload.get("message")
        or payload.get("body")
        or payload.get("email_body")
        or ""
    )

    if not email:
        return {"status": "ignored", "summary": "no email found in smartlead payload"}

    intent, auto_reply = _auto_reply_text(reply_text)

    with _session() as session:
        prospect = _find_prospect_by_email(session, email)

        if prospect is None:
            return {
                "status": "dry_run",
                "summary": "no matching prospect in db, generated reply only",
                "intent": intent,
                "generated_reply": auto_reply,
            }

        prospect.last_reply_state = intent

        if intent == "negative":
            prospect.status = "replied"
            summary = "negative reply; no auto response"

        elif auto_reply is None:
            prospect.status = "manual_review"
            summary = f"manual review ({intent})"

        elif not prospect.smartlead_campaign_id or not prospect.smartlead_lead_id:
            prospect.status = "manual_review"
            summary = "missing smartlead ids for auto reply"

        else:
            try:
                smartlead = SmartleadClient()
                await smartlead.reply_to_lead(
                    prospect.smartlead_campaign_id,
                    prospect.smartlead_lead_id,
                    auto_reply,
                )
                if intent in {"interested", "link_request", "pricing"}:
                    prospect.status = "interested"
                else:
                    prospect.status = "auto_replied"
                summary = f"auto replied ({intent})"
            except Exception as exc:
                prospect.status = "manual_review"
                summary = f"auto reply failed ({intent}): {exc}"

        _log_event(
            session,
            "smartlead_reply",
            prospect.external_id,
            summary,
            {
                "payload": payload,
                "intent": intent,
                "generated_reply": auto_reply,
            },
        )
        session.commit()

        return {
            "status": "processed",
            "summary": summary,
            "prospect_external_id": prospect.external_id,
            "intent": intent,
            "generated_reply": auto_reply,
        }


def handle_stripe_purchase_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("type", "")).strip()
    if event_type != "checkout.session.completed":
        return {"status": "ignored", "summary": f"stripe event {event_type or 'unknown'} ignored"}

    obj = payload.get("data", {}).get("object", {})
    email = str(
        obj.get("customer_details", {}).get("email")
        or obj.get("customer_email")
        or ""
    ).strip().lower()

    session_id = str(obj.get("id") or "").strip()
    amount_total = int(obj.get("amount_total") or 0)
    currency = str(obj.get("currency") or "usd").strip().lower()

    with _session() as session:
        prospect = _find_prospect_by_email(session, email) if email else None

        event_payload = {
            "email": email,
            "session_id": session_id,
            "amount_total": amount_total,
            "currency": currency,
            "raw": payload,
        }

        if prospect is None:
            session.add(
                AcquisitionEvent(
                    event_type="stripe_paid",
                    prospect_external_id=f"stripe:{email or session_id or 'unknown'}",
                    summary="stripe checkout completed (unmatched buyer)",
                    payload_json=json.dumps(event_payload, ensure_ascii=False),
                )
            )
            session.commit()
            return {
                "status": "processed",
                "summary": "stripe payment recorded (unmatched buyer)",
                "email": email,
                "amount_total": amount_total,
                "currency": currency,
            }

        prospect.stripe_status = "paid"
        prospect.status = "paid"
        _log_event(
            session,
            "stripe_paid",
            prospect.external_id,
            "stripe checkout completed",
            event_payload,
        )
        session.commit()
        return {
            "status": "processed",
            "summary": "prospect marked paid",
            "prospect_external_id": prospect.external_id,
            "email": email,
            "amount_total": amount_total,
            "currency": currency,
        }


def _extract_tally_email(payload: Dict[str, Any]) -> str:
    flat_candidates = [
        payload.get("email"),
        payload.get("contact_email"),
        payload.get("buyer_email"),
    ]
    for c in flat_candidates:
        if isinstance(c, str) and c.strip():
            return c.strip().lower()

    data = payload.get("data") or {}
    fields = data.get("fields") or payload.get("fields") or []
    for field in fields:
        key = str(field.get("key") or field.get("label") or "").lower()
        if "email" in key:
            value = field.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
    return ""


def handle_intake_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    email = _extract_tally_email(payload)
    if not email:
        return {"status": "ignored", "summary": "no email found in intake payload"}

    with _session() as session:
        prospect = _find_prospect_by_email(session, email)
        if prospect is None:
            return {"status": "ignored", "summary": "no matching prospect for intake email"}

        prospect.intake_status = "received"
        prospect.status = "intake_received"
        _log_event(session, "intake_received", prospect.external_id, "intake received", payload)
        session.commit()
        return {
            "status": "processed",
            "summary": "prospect marked intake_received",
            "prospect_external_id": prospect.external_id,
        }


async def tick_supervisor(send_live: bool = False) -> AcquisitionTickResult:
    enriched = await enrich_unsent_prospects(limit=10)
    queue_result = await queue_best_prospects_to_smartlead(
        send_live=send_live or settings.acq_auto_send,
        limit=settings.acq_daily_search_limit,
    )

    digest = acquisition_digest()

    return AcquisitionTickResult(
        searched=0,
        upserted=0,
        enriched=enriched,
        queued=queue_result["queued"],
        sent_live=queue_result["sent_live"],
        replied=digest["status_counts"].get("replied", 0),
        interested=digest["status_counts"].get("interested", 0),
        paid=digest["status_counts"].get("paid", 0),
        intake_received=digest["status_counts"].get("intake_received", 0),
        summary=f"tick complete: enriched={enriched}, queued={queue_result['queued']}, sent_live={queue_result['sent_live']}",
    )


def acquisition_digest() -> Dict[str, Any]:
    with _session() as session:
        rows = session.execute(
            select(AcquisitionProspect.status, func.count(AcquisitionProspect.id)).group_by(AcquisitionProspect.status)
        ).all()

        status_counts = {status: int(count) for status, count in rows}
        top_stmt = (
            select(AcquisitionProspect)
            .order_by(AcquisitionProspect.fit_score.desc(), AcquisitionProspect.created_at.desc())
            .limit(10)
        )
        top = list(session.execute(top_stmt).scalars().all())

    return {
        "status_counts": status_counts,
        "top_prospects": [
            {
                "external_id": p.external_id,
                "company_name": p.company_name,
                "contact_name": p.contact_name,
                "contact_email": p.contact_email,
                "title": p.title,
                "fit_score": p.fit_score,
                "fit_band": p.fit_band,
                "segment": p.segment,
                "status": p.status,
            }
            for p in top
        ],
    }
