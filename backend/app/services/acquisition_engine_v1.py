from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Dict, List, Literal


ICP_VERTICAL_HINTS = {
    "paid media",
    "seo",
    "ppc",
    "google ads",
    "meta ads",
    "web design",
    "lead gen",
    "lead generation",
    "growth",
    "cro",
    "agency",
    "roofing",
    "dental",
    "hvac",
    "med spa",
    "legal",
}

ICP_PAIN_HINTS = {
    "follow-up",
    "follow up",
    "post-call",
    "post call",
    "proposal",
    "crm",
    "handoff",
    "next step",
    "next steps",
    "admin drag",
    "momentum",
    "lag",
    "slow proposal",
    "recap",
    "notes",
}

LOW_FIT_HINTS = {
    "internal notes only",
    "ai summaries",
    "summary only",
    "self-serve",
    "self serve",
    "knowledge base",
    "call center",
    "transcription only",
    "meeting notes only",
    "internal summaries",
    "not sales calls",
}


@dataclass
class LeadCandidate:
    lead_id: str
    company_name: str
    website: str
    contact_name: str
    contact_email: str
    role: str
    vertical: str
    company_type: str
    source: str
    estimated_monthly_call_volume: int | None
    known_tools: List[str]
    notes: str = ""


@dataclass
class AcquisitionDecision:
    lead_id: str
    company_name: str
    fit_band: Literal["high_priority", "nurture", "discard"]
    score: int
    confidence: Literal["high", "medium", "low"]
    reasons: List[str]
    outreach_angle: str
    recommended_offer: str
    route: Literal["send_now", "nurture_later", "skip"]
    next_action: str
    outreach_subject: str
    outreach_body: str
    reply_classification: str
    founder_digest_line: str
    state_record: Dict[str, Any]


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_calls_per_week(value: Any) -> int | None:
    text = _norm(value).lower()
    if not text:
        return None
    m = re.search(r"(\d+)", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def from_lead_payload(payload: Dict[str, Any]) -> LeadCandidate:
    tools = payload.get("known_tools") or []
    if isinstance(tools, str):
        tools = [x.strip() for x in tools.split(",") if x.strip()]
    return LeadCandidate(
        lead_id=_norm(payload.get("lead_id") or payload.get("id")),
        company_name=_norm(payload.get("company_name")),
        website=_norm(payload.get("website")),
        contact_name=_norm(payload.get("contact_name")),
        contact_email=_norm(payload.get("contact_email") or payload.get("email")),
        role=_norm(payload.get("role")),
        vertical=_norm(payload.get("vertical")),
        company_type=_norm(payload.get("company_type")),
        source=_norm(payload.get("source")),
        estimated_monthly_call_volume=payload.get("estimated_monthly_call_volume"),
        known_tools=list(tools),
        notes=_norm(payload.get("notes")),
    )


def _combined_text(lead: LeadCandidate) -> str:
    return " ".join(
        [
            lead.company_name.lower(),
            lead.website.lower(),
            lead.contact_name.lower(),
            lead.role.lower(),
            lead.vertical.lower(),
            lead.company_type.lower(),
            " ".join(tool.lower() for tool in lead.known_tools),
            lead.notes.lower(),
        ]
    )


def score_lead_fit(lead: LeadCandidate) -> tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []

    combined = _combined_text(lead)

    if any(token in combined for token in ICP_VERTICAL_HINTS):
        score += 2
        reasons.append("vertical matches the current agency/operator wedge")
    else:
        reasons.append("vertical is not clearly in the current wedge")

    if "agency" in combined or "founder" in combined or "owner" in combined:
        score += 2
        reasons.append("lead looks founder-led or agency-like")
    elif "director" in combined or "head of" in combined:
        score += 1
        reasons.append("lead may still control commercial process")
    else:
        reasons.append("commercial decision-maker status is not clear")

    volume = lead.estimated_monthly_call_volume
    if volume is None:
        reasons.append("call volume is unknown")
    elif 20 <= volume <= 200:
        score += 3
        reasons.append("call volume suggests repeated post-call execution pain")
    elif 8 <= volume < 20:
        score += 2
        reasons.append("call volume is enough to justify a paid pilot")
    elif 1 <= volume < 8:
        reasons.append("call volume may be too low for the strongest ROI case")
    elif volume > 200:
        reasons.append("very high call volume may require a more complex rollout")
    else:
        score -= 2
        reasons.append("call volume does not currently support the offer")

    pain_hits = [token for token in ICP_PAIN_HINTS if token in combined]
    if pain_hits:
        score += 3
        reasons.append("lead notes point to post-call execution or follow-through pain")
    else:
        reasons.append("post-call execution pain is not yet explicit")

    if lead.contact_email and lead.website:
        score += 1
        reasons.append("contact and company fields are complete enough for outreach")
    else:
        reasons.append("lead record is incomplete")

    low_fit_hits = [token for token in LOW_FIT_HINTS if token in combined]
    if low_fit_hits:
        score -= 6
        reasons.append("lead sounds closer to a summaries/transcription use case")

    if "enterprise" in combined and "agency" not in combined:
        score -= 2
        reasons.append("lead sounds broader and less founder-led than the wedge")

    return score, reasons


def classify_lead(lead: LeadCandidate) -> tuple[Literal["high_priority", "nurture", "discard"], Literal["high", "medium", "low"], int, List[str]]:
    score, reasons = score_lead_fit(lead)

    if score >= 6:
        return "high_priority", "high", score, reasons
    if score >= 2:
        return "nurture", "medium", score, reasons
    return "discard", ("high" if score <= -2 else "medium"), score, reasons


def choose_outreach_angle(lead: LeadCandidate, fit_band: str) -> str:
    combined = _combined_text(lead)
    if "proposal" in combined:
        return "slow proposal turnaround after calls"
    if "crm" in combined or "handoff" in combined:
        return "cleaner post-call handoff into CRM and next steps"
    if "follow-up" in combined or "follow up" in combined:
        return "faster post-call follow-through"
    if "booked consults" in combined or "consult" in combined:
        return "turning more consult conversations into cleaner next actions"
    if fit_band == "high_priority":
        return "reducing post-call admin drag"
    return "sharpening post-call follow-up"


def choose_offer(fit_band: str) -> str:
    if fit_band == "high_priority":
        return "$1 follow-up email"
    if fit_band == "nurture":
        return "call example review"
    return "no current offer"


def choose_route(fit_band: str) -> Literal["send_now", "nurture_later", "skip"]:
    if fit_band == "high_priority":
        return "send_now"
    if fit_band == "nurture":
        return "nurture_later"
    return "skip"


def next_action_for_route(route: str) -> str:
    if route == "send_now":
        return "Send outbound message now and track reply state."
    if route == "nurture_later":
        return "Save lead for later or ask for stronger context before outreach."
    return "Do not send outreach. Leave out of active queue."


def render_outreach(lead: LeadCandidate, fit_band: str, outreach_angle: str, recommended_offer: str) -> tuple[str, str]:
    first_name = lead.contact_name.split(" ")[0] if lead.contact_name else "there"
    company = lead.company_name or "your team"

    if fit_band == "high_priority":
        subject = "one follow-up email"
        body = (
            f"Hi {first_name},\n\n"
            "I run RelayBrief at relaybrief.com.\n\n"
            "It turns rough notes from one sales or client call into one clean follow-up email.\n\n"
            "No download, install, account, password, or card form. If you send one rough note, I reply with a short follow-up email preview first. Pay $1 only if it helps.\n\n"
            "- Alan"
        )
        return subject, body

    if fit_band == "nurture":
        subject = "RelayBrief fit check"
        body = (
            f"Hi {first_name},\n\n"
            "I run RelayBrief at relaybrief.com.\n\n"
            "It may be relevant if rough sales or client-call notes are turning into delayed follow-up. You can send one rough note and get a short follow-up email preview first. Pay $1 only if it helps.\n\n"
            "No download, install, account, password, or card form. If that is not relevant, no need to reply.\n\n"
            "- Alan"
        )
        return subject, body

    subject = "RelayBrief fit check"
    body = (
        f"Hi {first_name},\n\n"
        "I run RelayBrief at relaybrief.com. It turns one rough call note into one follow-up email. Preview first; $1 only if useful.\n\n"
        "This only makes sense if post-call follow-up is actually slow or annoying. If not, no need to reply.\n\n"
        "- Alan"
    )
    return subject, body


def classify_reply(text: str) -> str:
    norm = _norm(text).lower()
    if not norm:
        return "no_reply"
    if any(token in norm for token in ["yes", "interested", "let's do it", "lets do it", "sounds good", "send more"]):
        return "interested"
    if any(token in norm for token in ["not now", "circle back", "later", "next quarter", "busy"]):
        return "not_now"
    if any(token in norm for token in ["not a fit", "no thanks", "remove me", "unsubscribe"]):
        return "not_fit"
    if any(token in norm for token in ["how much", "price", "pricing", "what does it cost", "sample", "example"]):
        return "needs_info"
    return "unknown"


def build_state_record(
    lead: LeadCandidate,
    fit_band: str,
    score: int,
    confidence: str,
    outreach_angle: str,
    recommended_offer: str,
    route: str,
    next_action: str,
) -> Dict[str, Any]:
    return {
        "lead_id": lead.lead_id,
        "company_name": lead.company_name,
        "contact_name": lead.contact_name,
        "contact_email": lead.contact_email,
        "website": lead.website,
        "source": lead.source,
        "fit_band": fit_band,
        "fit_score": score,
        "confidence": confidence,
        "outreach_angle": outreach_angle,
        "recommended_offer": recommended_offer,
        "route": route,
        "next_action": next_action,
        "estimated_monthly_call_volume": lead.estimated_monthly_call_volume,
    }


def founder_digest_line(lead: LeadCandidate, fit_band: str, route: str, outreach_angle: str) -> str:
    volume = (
        "unknown volume"
        if lead.estimated_monthly_call_volume is None
        else f"{lead.estimated_monthly_call_volume} calls/mo"
    )
    return f"{lead.company_name} | {fit_band} | {route} | {volume} | angle: {outreach_angle}"


def run_acquisition_engine_v1(payload: Dict[str, Any]) -> AcquisitionDecision:
    lead = from_lead_payload(payload)
    fit_band, confidence, score, reasons = classify_lead(lead)
    outreach_angle = choose_outreach_angle(lead, fit_band)
    recommended_offer = choose_offer(fit_band)
    route = choose_route(fit_band)
    next_action = next_action_for_route(route)
    subject, outreach_body = render_outreach(lead, fit_band, outreach_angle, recommended_offer)
    digest = founder_digest_line(lead, fit_band, route, outreach_angle)
    state_record = build_state_record(
        lead=lead,
        fit_band=fit_band,
        score=score,
        confidence=confidence,
        outreach_angle=outreach_angle,
        recommended_offer=recommended_offer,
        route=route,
        next_action=next_action,
    )

    return AcquisitionDecision(
        lead_id=lead.lead_id,
        company_name=lead.company_name,
        fit_band=fit_band,
        score=score,
        confidence=confidence,
        reasons=reasons,
        outreach_angle=outreach_angle,
        recommended_offer=recommended_offer,
        route=route,
        next_action=next_action,
        outreach_subject=subject,
        outreach_body=outreach_body,
        reply_classification="no_reply",
        founder_digest_line=digest,
        state_record=state_record,
    )


def acquisition_decision_to_dict(decision: AcquisitionDecision) -> Dict[str, Any]:
    return asdict(decision)
