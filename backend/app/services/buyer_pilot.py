
from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from typing import Any, Literal

from sqlalchemy import select

from app.core.config import relay_costs_paused, relay_paused_response, settings
from app.db.base import SessionLocal
from app.integrations.resend_client import ResendClient
from app.models.funnel import FunnelState, Lead
from app.services.guardrails import clean_agency_name, clean_bottleneck, clean_calls_per_week, clean_website

BUYER_FIELD_MAP = {
    "Work Email": "email",
    "Agency Name": "agency_name",
    "Website": "website",
    "How many discovery or sales calls do you handle per week?": "calls_per_week",
    "How many discovery or sales calls per week?": "calls_per_week",
    "Biggest post-call bottleneck": "bottleneck",
    "Optional rough follow-up draft or a few bullets": "rough_notes",
}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(_stringify(v) for v in value if _stringify(v))
    if isinstance(value, dict):
        if "value" in value:
            return _stringify(value["value"])
        if "label" in value:
            return _stringify(value["label"])
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _field_map(payload: dict[str, Any]) -> dict[str, str]:
    data = payload.get("data") or {}
    fields = data.get("fields") or []
    out: dict[str, str] = {}
    for item in fields:
        label = _stringify(item.get("label") or item.get("title") or item.get("key"))
        if label:
            out[label] = _stringify(item.get("value"))
    return out


def _normalize_buyer_payload(payload: dict[str, Any]) -> dict[str, str]:
    raw = _field_map(payload)
    normalized = {
        "email": "",
        "agency_name": "",
        "website": "",
        "calls_per_week": "",
        "bottleneck": "",
        "rough_notes": "",
    }
    for label, key in BUYER_FIELD_MAP.items():
        if raw.get(label):
            normalized[key] = raw[label]
    normalized["agency_name"] = clean_agency_name(normalized["agency_name"])
    normalized["website"] = clean_website(normalized["website"])
    normalized["calls_per_week"] = clean_calls_per_week(normalized["calls_per_week"])
    normalized["bottleneck"] = clean_bottleneck(normalized["bottleneck"])
    normalized["rough_notes"] = _stringify(normalized["rough_notes"])
    return normalized


def _sample_pdf_url() -> str:
    return (
        getattr(settings, "sample_pdf_url", "")
            or "https://relaybrief.com/sample.pdf"
    )


def _calls_phrase(calls: str) -> str:
    if not calls:
        return ""
    value = int(calls)
    noun = "call" if value == 1 else "calls"
    return f"You’re handling about {value} {noun} per week."


def _clean_fragment(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    cleaned = re.sub(r"[.]{2,}$", "", cleaned)
    cleaned = re.sub(r"[!?]{2,}", "", cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned


def _infer_fit_band(fields: dict[str, str]) -> Literal["good_fit", "standard_fit", "lower_fit"]:
    calls = int(fields.get("calls_per_week") or 0)
    bottleneck = (fields.get("bottleneck") or "").lower()
    rough_notes = (fields.get("rough_notes") or "").lower()
    agency = (fields.get("agency_name") or "").lower()
    combined = " ".join([bottleneck, rough_notes, agency])

    low_fit_markers = [
        "ai summaries",
        "internal notes",
        "summary only",
        "self-serve",
        "self serve",
        "enterprise",
        "large enterprise",
        "lightweight summaries",
    ]
    strong_fit_markers = [
        "founder-led",
        "founder led",
        "crm",
        "proposal",
        "follow-up",
        "follow up",
        "admin drag",
        "discovery",
        "post-call",
        "post call",
    ]

    if any(marker in combined for marker in low_fit_markers):
        return "lower_fit"
    if calls and calls <= 2:
        return "lower_fit"
    if any(marker in combined for marker in strong_fit_markers):
        return "good_fit"
    if calls and calls >= 4:
        return "standard_fit"
    return "standard_fit"


def _rewrite_bottleneck(text: str) -> str:
    cleaned = _clean_fragment(text or "delayed follow-up after calls")
    lowered = cleaned.lower()

    rewrites = {
        "we lose momentum after the call and proposals take too long": "lost momentum after calls and slow proposal turnaround",
        "follow-up quality drops after discovery calls, and next steps often get delayed": "follow-up quality dropping after discovery calls and delayed next steps",
        "follow-up quality drops after discovery calls and next steps often get delayed": "follow-up quality dropping after discovery calls and delayed next steps",
        "need ai summaries for internal notes": "AI summaries for internal notes rather than post-call execution",
    }
    for key, value in rewrites.items():
        if lowered == key:
            return value

    cleaned = re.sub(r"^we\s+", "", lowered)
    cleaned = re.sub(r"\bproposals take too long\b", "slow proposal turnaround", cleaned)
    cleaned = re.sub(r"\blose momentum after the call\b", "lost momentum after calls", cleaned)
    cleaned = re.sub(r"\bafter the call\b", "after calls", cleaned)
    cleaned = re.sub(r"\bneed ai summaries for internal notes\b", "AI summaries for internal notes rather than post-call execution", cleaned)
    cleaned = re.sub(r"\s+,", ",", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" .")
    return cleaned or "delayed follow-up after calls"


def _paragraph(text: str) -> str:
    return f"<p style='margin:0 0 18px 0; line-height:1.6;'>{html.escape(text)}</p>"


def _link_paragraph(label: str, url: str) -> str:
    return (
        "<p style='margin:0 0 18px 0; line-height:1.6;'>"
        f"<a href='{html.escape(url, quote=True)}'>{html.escape(label)}</a>"
        "</p>"
    )


def _wrap_email(blocks: list[str]) -> str:
    return (
        "<div style='font-family:Arial,Helvetica,sans-serif;font-size:16px;color:#1f1f1f;'>"
        + "".join(blocks)
        + "</div>"
    )


def _build_good_fit_email(fields: dict[str, str]) -> tuple[str, str]:
    agency = fields.get("agency_name") or "your agency"
    calls_line = _calls_phrase(fields.get("calls_per_week", ""))
    friction = _rewrite_bottleneck(fields.get("bottleneck", ""))
    sample_pdf_url = _sample_pdf_url()

    blocks = [
        _paragraph("Hi - Alan here."),
        _paragraph(f"Thanks for sending details for {agency}."),
    ]
    if calls_line:
        blocks.append(_paragraph(calls_line))
    blocks.append(_paragraph(f"The main friction is {friction}."))
    blocks.append(_paragraph("Here is an example output:"))
    blocks.append(_link_paragraph("View example output", sample_pdf_url))
    blocks.append(_paragraph("If this looks right, reply with one stuck client email, last message, rough draft, or a few bullets. I will send one follow-up email first. No payment before preview."))
    blocks.append(_paragraph("- Alan"))
    return "stuck client email reply", _wrap_email(blocks)


def _build_lower_fit_email(fields: dict[str, str]) -> tuple[str, str]:
    agency = fields.get("agency_name") or "your agency"
    sample_pdf_url = _sample_pdf_url()
    blocks = [
        _paragraph("Hi - Alan here."),
        _paragraph(f"Thanks for sending details for {agency}."),
        _paragraph(
            "Based on this request, I am not yet sure this is the right fit. "
            "This is strongest when a founder-led team has one stuck client email, a real last reply, "
            "and needs one next email to send."
        ),
        _paragraph("Here is an example output:"),
        _link_paragraph("View example output", sample_pdf_url),
        _paragraph(
            "If that is still the workflow you want, reply with the stuck client email, last reply, rough draft, or a few bullets "
            "and I will send one follow-up email before any payment."
        ),
        _paragraph("- Alan"),
    ]
    return "stuck client email reply", _wrap_email(blocks)


def _build_buyer_email(fields: dict[str, str]) -> tuple[str, str]:
    fit_band = _infer_fit_band(fields)
    if fit_band == "lower_fit":
        return _build_lower_fit_email(fields)
    return _build_good_fit_email(fields)


def _send_html_email(to_email: str, subject: str, html_body: str) -> dict[str, Any]:
    if relay_costs_paused():
        return relay_paused_response("buyer_pilot_send_html_email")

    client = ResendClient()
    return client.send_outbound_email(to_email=to_email, subject=subject, html=html_body)


def _upsert_lead(fields: dict[str, str]) -> dict[str, Any]:
    email = fields.get("email", "").strip().lower()
    company = fields.get("agency_name") or "your agency"
    note_lines = [
        f"website={fields.get('website', '')}",
        f"calls_per_week={fields.get('calls_per_week', '')}",
        f"bottleneck={fields.get('bottleneck', '')}",
        f"captured_at={datetime.now(timezone.utc).isoformat()}",
    ]
    notes = "\n".join(line for line in note_lines if line.split("=", 1)[1])

    with SessionLocal() as session:
        existing = session.execute(select(Lead).where(Lead.email == email)).scalar_one_or_none()
        if existing is None:
            existing = Lead(
                company=company,
                contact_name="",
                email=email,
                niche="founder-led agency",
                state=FunnelState.INTERESTED,
                notes=notes,
            )
            session.add(existing)
        else:
            existing.company = company or existing.company
            existing.state = FunnelState.INTERESTED
            existing.notes = notes or existing.notes
        session.commit()
        session.refresh(existing)
        return {"lead_id": existing.id, "state": existing.state.value}


def process_buyer_pilot_submission(payload: dict[str, Any]) -> dict[str, Any]:
    submission_id = _stringify((payload.get("data") or {}).get("submissionId")) or _stringify(payload.get("submission_id"))
    if relay_costs_paused():
        return {
            **relay_paused_response("process_buyer_pilot_submission"),
            "submission_id": submission_id,
        }

    fields = _normalize_buyer_payload(payload)
    email = fields.get("email", "").strip().lower()
    if not email:
        raise RuntimeError("Buyer pilot payload missing Work Email")

    lead_result = _upsert_lead(fields)
    subject, html_body = _build_buyer_email(fields)
    send_result = _send_html_email(email, subject, html_body)

    result = {
        "submission_id": submission_id,
        "lead": lead_result,
        "email": email,
        "subject": subject,
        "send_result": send_result,
    }
    print(f"[BUYER PILOT DONE] {json.dumps(result, default=str, ensure_ascii=False)}")
    return result
