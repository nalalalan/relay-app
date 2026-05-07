from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Dict, List, Literal


LaunchType = Literal["audit_first", "pilot_first", "clarify_first"]


@dataclass
class QualifiedOpportunity:
    opportunity_id: str
    company_name: str
    contact_name: str
    contact_email: str
    fit_band: str
    route: str
    vertical: str
    estimated_monthly_call_volume: int | None
    known_pain: str
    notes: str
    reply_state: str
    reply_text: str = ""


@dataclass
class ProposalLaunchDecision:
    opportunity_id: str
    company_name: str
    launch_type: LaunchType
    confidence: Literal["high", "medium", "low"]
    recommended_offer: str
    recommended_price_guidance: str
    why_this_offer: str
    best_next_commercial_move: str
    missing_info: List[str]
    scope_outline: List[str]
    buyer_email_subject: str
    buyer_email_body: str
    internal_operator_note: str
    founder_digest_line: str
    state_record: Dict[str, Any]


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _looks_like_local_service(text: str) -> bool:
    norm = text.lower()
    return any(token in norm for token in ["roofing", "dental", "implant", "invisalign", "hvac", "med spa", "legal"])


def _looks_like_agency(text: str) -> bool:
    norm = text.lower()
    return any(token in norm for token in ["agency", "paid media", "seo", "ppc", "growth", "cro"])


def _mentions_audit(text: str) -> bool:
    norm = text.lower()
    return any(token in norm for token in ["audit", "review", "diagnostic", "discovery sprint"])


def _mentions_proposal_delay(text: str) -> bool:
    norm = text.lower()
    return any(token in norm for token in ["proposal", "slow proposal", "proposal lag", "next-step proposal", "proposal direction"])


def _mentions_tracking_or_attribution(text: str) -> bool:
    norm = text.lower()
    return any(token in norm for token in ["tracking", "attribution", "callrail", "crm", "lead quality", "google ads"])


def parse_opportunity(payload: Dict[str, Any]) -> QualifiedOpportunity:
    return QualifiedOpportunity(
        opportunity_id=_norm(payload.get("opportunity_id") or payload.get("lead_id") or payload.get("id")),
        company_name=_norm(payload.get("company_name")),
        contact_name=_norm(payload.get("contact_name")),
        contact_email=_norm(payload.get("contact_email") or payload.get("email")),
        fit_band=_norm(payload.get("fit_band")),
        route=_norm(payload.get("route")),
        vertical=_norm(payload.get("vertical")),
        estimated_monthly_call_volume=payload.get("estimated_monthly_call_volume"),
        known_pain=_norm(payload.get("known_pain") or payload.get("biggest_post_call_bottleneck") or payload.get("pain")),
        notes=_norm(payload.get("notes")),
        reply_state=_norm(payload.get("reply_state") or payload.get("state")),
        reply_text=_norm(payload.get("reply_text")),
    )


def choose_launch_type(opportunity: QualifiedOpportunity) -> tuple[LaunchType, str, str]:
    combined = " ".join(
        [
            opportunity.company_name,
            opportunity.vertical,
            opportunity.known_pain,
            opportunity.notes,
            opportunity.reply_text,
        ]
    ).lower()

    # 1) If they are still asking basic questions, do not force a paid step yet.
    if opportunity.reply_state == "replied_needs_info":
        return "clarify_first", "Clarify-first fit check", "Need missing details before proposing paid work."

    # 2) Deterministic override:
    # agency-like + pilot_path should prefer pilot-first unless the notes explicitly say audit.
    if opportunity.route == "pilot_path" and _looks_like_agency(combined) and not _mentions_audit(combined):
        return "pilot_first", "5-call pilot", "The opportunity already looks like a strong post-call execution fit."

    # 3) Agency-like + proposal-follow-through pain should also prefer pilot-first.
    if _looks_like_agency(combined) and _mentions_proposal_delay(combined) and not _mentions_audit(combined):
        return "pilot_first", "5-call pilot", "The main pain looks like post-call execution and proposal follow-through."

    # 4) Local-service diagnostic work should prefer audit-first.
    if _looks_like_local_service(combined) and (_mentions_audit(combined) or _mentions_tracking_or_attribution(combined)):
        return "audit_first", "Short paid audit", "The notes point to diagnostic work before a larger commitment."

    # 5) Explicit audit language still wins.
    if _mentions_audit(combined):
        return "audit_first", "Short paid audit", "The notes point to diagnostic work before a larger commitment."

    # 6) Tracking / attribution problems without agency execution signals can lean audit-first.
    if _mentions_tracking_or_attribution(combined) and not _looks_like_agency(combined):
        return "audit_first", "Short paid audit", "The notes point to diagnostic work before a larger commitment."

    # 7) Remaining agency-like cases fall to pilot-first.
    if _mentions_proposal_delay(combined) or _looks_like_agency(combined):
        return "pilot_first", "5-call pilot", "The main pain looks like post-call execution and proposal follow-through."

    return "clarify_first", "Clarify-first fit check", "There is not enough commercial specificity yet."


def price_guidance_for(launch_type: LaunchType, opportunity: QualifiedOpportunity) -> str:
    if launch_type == "audit_first":
        if _looks_like_local_service(" ".join([opportunity.vertical, opportunity.notes])):
            return "$500-$1,500 diagnostic audit depending on complexity"
        return "$500-$2,000 diagnostic audit depending on call complexity and tracking depth"
    if launch_type == "pilot_first":
        return "$250-$750 fixed-price pilot depending on output depth and turnaround"
    return "No price yet — clarify scope first"


def missing_info_for(launch_type: LaunchType, opportunity: QualifiedOpportunity) -> List[str]:
    combined = " ".join([opportunity.known_pain, opportunity.notes, opportunity.reply_text]).lower()
    missing: List[str] = []

    if "budget" not in combined:
        missing.append("Budget comfort for the initial paid step")
    if "timeline" not in combined and "this week" not in combined and "next week" not in combined:
        missing.append("Decision timeline")
    if "primary contact" not in combined and opportunity.contact_name == "":
        missing.append("Primary contact / decision-maker confirmation")

    if launch_type == "audit_first":
        if "google ads" in combined and "account access" not in combined:
            missing.append("Whether account access can be granted for the audit")
        if "lead quality" in combined and "metric" not in combined:
            missing.append("What counts as a good lead or booked consult")
    elif launch_type == "pilot_first":
        if opportunity.estimated_monthly_call_volume is None:
            missing.append("Expected monthly sales-call volume")
        if "crm" not in combined:
            missing.append("What system the team uses after calls")
    else:
        missing.append("One recent real call example")

    deduped = []
    seen = set()
    for item in missing:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def scope_outline_for(launch_type: LaunchType, opportunity: QualifiedOpportunity) -> List[str]:
    combined = " ".join([opportunity.known_pain, opportunity.notes, opportunity.reply_text]).lower()

    if launch_type == "audit_first":
        outline = [
            "Review the current post-call follow-up flow and identify where momentum drops.",
            "Review lead-quality signals, tracking, and attribution gaps tied to current demand channels.",
            "Deliver a short diagnosis with the highest-priority fixes and the recommended next commercial move.",
        ]
        if "google ads" in combined:
            outline.insert(1, "Inspect the Google Ads-to-lead-quality chain and identify where lead intent is getting lost.")
        return outline

    if launch_type == "pilot_first":
        return [
            "Process a fixed batch of real sales calls through the Alan Operator workflow.",
            "Generate recap, next steps, follow-up draft, and CRM-ready handoff for each call.",
            "Review the output quality and decide whether the workflow should expand into ongoing use.",
        ]

    return [
        "Review one real example before making a paid recommendation.",
        "Clarify the post-call bottleneck, current system, and desired outcome.",
        "Decide whether the right next step is a pilot, audit, or a different entry point.",
    ]


def best_next_move_for(launch_type: LaunchType) -> str:
    mapping = {
        "audit_first": "Offer a short paid diagnostic audit as the next step.",
        "pilot_first": "Offer a fixed-size 5-call pilot as the next step.",
        "clarify_first": "Ask for one recent real call example before proposing paid work.",
    }
    return mapping[launch_type]


def buyer_email_for(opportunity: QualifiedOpportunity, launch_type: LaunchType, offer_name: str, why: str, missing_info: List[str]) -> tuple[str, str]:
    first_name = opportunity.contact_name.split(" ")[0] if opportunity.contact_name else "there"
    company = opportunity.company_name or "your team"

    if launch_type == "clarify_first":
        subject = f"{company} — best next step"
        body = (
            f"Hi {first_name},\n\n"
            "Based on what I have so far, I would not jump straight into a paid step yet. "
            "The cleanest move is to look at one recent real call example first so I can tell you whether the right entry point is a short audit, a 5-call pilot, or something else.\n\n"
            "If you send one concrete example, I can point you to the right next step quickly.\n\n"
            "— Alan"
        )
        return subject, body

    subject = f"{company} — recommended next step"
    missing_block = ""
    if missing_info:
        missing_block = "\n\nBefore locking this in, I would still want to confirm:\n- " + "\n- ".join(missing_info[:3])

    body = (
        f"Hi {first_name},\n\n"
        f"Current move: {offer_name.lower()}. "
        f"{why}{missing_block}\n\n"
        "If you want, I can map the exact first scope and next action from here.\n\n"
        "— Alan"
    )
    return subject, body


def internal_note_for(
    opportunity: QualifiedOpportunity,
    launch_type: LaunchType,
    offer_name: str,
    price_guidance: str,
    best_move: str,
    missing_info: List[str],
    scope_outline: List[str],
) -> str:
    lines = [
        f"Opportunity: {opportunity.company_name}",
        f"Reply state: {opportunity.reply_state}",
        f"Recommended next step: {offer_name}",
        f"Launch type: {launch_type}",
        f"Price guidance: {price_guidance}",
        f"Best move: {best_move}",
        "",
        "Scope outline:",
    ]
    for item in scope_outline:
        lines.append(f"- {item}")
    if missing_info:
        lines.extend(["", "Missing info:"])
        for item in missing_info:
            lines.append(f"- {item}")
    return "\n".join(lines)


def founder_digest_line(opportunity: QualifiedOpportunity, offer_name: str, launch_type: LaunchType) -> str:
    return f"{opportunity.company_name} | {opportunity.reply_state} | {launch_type} | next: {offer_name}"


def state_record_for(
    opportunity: QualifiedOpportunity,
    launch_type: LaunchType,
    offer_name: str,
    price_guidance: str,
    best_move: str,
) -> Dict[str, Any]:
    return {
        "opportunity_id": opportunity.opportunity_id,
        "company_name": opportunity.company_name,
        "reply_state": opportunity.reply_state,
        "launch_type": launch_type,
        "recommended_offer": offer_name,
        "price_guidance": price_guidance,
        "best_next_commercial_move": best_move,
    }


def run_proposal_audit_launcher_v1(payload: Dict[str, Any]) -> ProposalLaunchDecision:
    opportunity = parse_opportunity(payload)
    launch_type, offer_name, why = choose_launch_type(opportunity)
    price_guidance = price_guidance_for(launch_type, opportunity)
    missing_info = missing_info_for(launch_type, opportunity)
    scope_outline = scope_outline_for(launch_type, opportunity)
    best_move = best_next_move_for(launch_type)
    subject, buyer_email = buyer_email_for(opportunity, launch_type, offer_name, why, missing_info)
    internal_note = internal_note_for(
        opportunity=opportunity,
        launch_type=launch_type,
        offer_name=offer_name,
        price_guidance=price_guidance,
        best_move=best_move,
        missing_info=missing_info,
        scope_outline=scope_outline,
    )
    digest = founder_digest_line(opportunity, offer_name, launch_type)
    state_record = state_record_for(opportunity, launch_type, offer_name, price_guidance, best_move)

    confidence = "high" if launch_type != "clarify_first" else "medium"

    return ProposalLaunchDecision(
        opportunity_id=opportunity.opportunity_id,
        company_name=opportunity.company_name,
        launch_type=launch_type,
        confidence=confidence,
        recommended_offer=offer_name,
        recommended_price_guidance=price_guidance,
        why_this_offer=why,
        best_next_commercial_move=best_move,
        missing_info=missing_info,
        scope_outline=scope_outline,
        buyer_email_subject=subject,
        buyer_email_body=buyer_email,
        internal_operator_note=internal_note,
        founder_digest_line=digest,
        state_record=state_record,
    )


def proposal_launch_to_dict(decision: ProposalLaunchDecision) -> Dict[str, Any]:
    return asdict(decision)
