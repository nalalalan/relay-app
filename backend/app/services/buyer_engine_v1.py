from __future__ import annotations

from dataclasses import asdict, dataclass
from html import escape
import re
from typing import Any, Dict, List, Literal


FIT_VERTICAL_HINTS = {
    "paid media",
    "seo",
    "web design",
    "growth",
    "cro",
    "ppc",
    "google ads",
    "meta ads",
    "roofing",
    "dental",
    "invisalign",
    "implant",
    "hvac",
    "med spa",
    "agency",
}

FIT_FRICTION_HINTS = {
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
class BuyerIntake:
    work_email: str
    agency_name: str
    website: str
    calls_per_week: int | None
    biggest_post_call_bottleneck: str
    recent_call_notes: str = ""


@dataclass
class BuyerDecision:
    fit_band: str
    route: str
    score: int
    confidence: str
    main_friction: str
    reasons: List[str]
    next_action: str
    subject: str
    text_body: str
    html_body: str
    founder_digest: str
    lead_record: Dict[str, Any]


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


def from_form_payload(payload: Dict[str, Any]) -> BuyerIntake:
    return BuyerIntake(
        work_email=_norm(payload.get("work_email") or payload.get("email")),
        agency_name=_norm(payload.get("agency_name")),
        website=_norm(payload.get("website")),
        calls_per_week=parse_calls_per_week(
            payload.get("calls_per_week")
            or payload.get("how_many_calls")
            or payload.get("discovery_calls_per_week")
        ),
        biggest_post_call_bottleneck=_norm(
            payload.get("biggest_post_call_bottleneck") or payload.get("bottleneck")
        ),
        recent_call_notes=_norm(
            payload.get("recent_call_notes")
            or payload.get("optional_rough_notes")
            or payload.get("notes")
        ),
    )


def _score_fit(intake: BuyerIntake) -> tuple[int, List[str], str]:
    score = 0
    reasons: List[str] = []

    combined = " ".join(
        [
            intake.agency_name.lower(),
            intake.website.lower(),
            intake.biggest_post_call_bottleneck.lower(),
            intake.recent_call_notes.lower(),
        ]
    )

    calls = intake.calls_per_week
    if calls is None:
        reasons.append("call volume is still unknown")
    elif 3 <= calls <= 30:
        score += 3
        reasons.append("call volume is enough to test the $1 packet")
    elif 1 <= calls <= 2:
        score += 1
        reasons.append("call volume is low but still testable")
    elif 31 <= calls <= 50:
        score += 1
        reasons.append("call volume is high enough to justify process cleanup")
    elif calls > 50:
        score -= 1
        reasons.append("very high call volume may need a more structured rollout than a simple pilot")
    else:
        score -= 2
        reasons.append("call volume does not currently support the packet")

    if any(token in combined for token in FIT_VERTICAL_HINTS):
        score += 2
        reasons.append("vertical and offer type look close to the intended agency buyer")

    matched_friction = [token for token in FIT_FRICTION_HINTS if token in combined]
    if matched_friction:
        score += 3
        reasons.append("the request clearly points to post-call execution pain")
    else:
        score -= 1
        reasons.append("the request does not clearly describe a post-call execution problem")

    if intake.recent_call_notes and len(intake.recent_call_notes) >= 30:
        score += 1
        reasons.append("there is enough context to show how the workflow would be used")
    elif intake.recent_call_notes:
        reasons.append("rough draft or bullets exist, but they are still thin")

    low_fit_hits = [token for token in LOW_FIT_HINTS if token in combined]
    if low_fit_hits:
        score -= 5
        reasons.append("the request sounds closer to an internal-notes or self-serve use case")

    if "enterprise" in combined and not matched_friction:
        score -= 2
        reasons.append("the request sounds broader and less founder-led than the ideal buyer")

    friction = intake.biggest_post_call_bottleneck or "post-call follow-up and proposal direction"

    return score, reasons, friction


def classify_fit(intake: BuyerIntake) -> tuple[str, str, str, int, List[str], str]:
    score, reasons, friction = _score_fit(intake)

    if score >= 6:
        band = "strong_fit"
        route = "pilot_path"
        confidence = "high"
    elif score >= 3:
        band = "maybe_fit"
        route = "clarify_path"
        confidence = "medium"
    else:
        band = "low_fit"
        route = "soft_disqualify_path"
        confidence = "medium" if score >= 1 else "high"

    return band, route, confidence, score, reasons, friction


def _html_link(url: str, label: str) -> str:
    return f'<a href="{escape(url, quote=True)}">{escape(label)}</a>'


def _text_link(url: str, label: str) -> str:
    return f"{label}\n{url}"


def render_buyer_reply(
    intake: BuyerIntake,
    fit_band: str,
    route: str,
    friction: str,
    sample_packet_url: str,
) -> tuple[str, str, str]:
    agency = intake.agency_name or "your agency"
    calls_text = (
        f"You're handling about {intake.calls_per_week} calls per week."
        if intake.calls_per_week is not None
        else "Your exact call volume is still unclear from the request."
    )

    sample_html = _html_link(sample_packet_url, "View example output")
    sample_text = _text_link(sample_packet_url, "View example output")

    if route == "pilot_path":
        subject = "unanswered quote follow-up"
        text_body = (
            "Hi - Alan here.\n\n"
            f"Thanks for sending details for {agency}.\n\n"
            f"{calls_text}\n\n"
            f"The main friction is {friction}.\n\n"
            "Here is an example output:\n\n"
            f"{sample_text}\n\n"
            "If this looks right, reply with one unanswered quote, last reply, rough draft, or a few bullets. I will send one follow-up email first. No payment before preview.\n\n"
            "- Alan"
        )
        html_body = (
            f"<p>Hi - Alan here.</p>"
            f"<p>Thanks for sending details for {escape(agency)}.</p>"
            f"<p>{escape(calls_text)}</p>"
            f"<p>The main friction is {escape(friction)}.</p>"
            "<p>Here is an example output:</p>"
            f"<p>{sample_html}</p>"
            "<p>If this looks right, reply with one unanswered quote, last reply, rough draft, or a few bullets. I will send one follow-up email first. No payment before preview.</p>"
            "<p>- Alan</p>"
        )
        return subject, text_body, html_body

    if route == "clarify_path":
        subject = "unanswered quote follow-up"
        text_body = (
            "Hi - Alan here.\n\n"
            f"Thanks for sending details for {agency}.\n\n"
            "Based on this request, I am not yet sure this is the right fit. "
            "This is strongest when a founder-led team has one unanswered quote, a real last reply, "
            "and needs one next email to send.\n\n"
            "Here is an example output:\n\n"
            f"{sample_text}\n\n"
            "If that is still the workflow you want, reply with one unanswered quote, last reply, rough draft, or a few bullets and I will send one follow-up email before any payment.\n\n"
            "- Alan"
        )
        html_body = (
            f"<p>Hi - Alan here.</p>"
            f"<p>Thanks for sending details for {escape(agency)}.</p>"
            "<p>Based on this request, I am not yet sure this is the right fit. "
            "This is strongest when a founder-led team has one unanswered quote, a real last reply, "
            "and needs one next email to send.</p>"
            "<p>Here is an example output:</p>"
            f"<p>{sample_html}</p>"
            "<p>If that is still the workflow you want, reply with one unanswered quote, last reply, rough draft, or a few bullets and I will send one follow-up email before any payment.</p>"
            "<p>- Alan</p>"
        )
        return subject, text_body, html_body

    subject = "unanswered quote follow-up"
    text_body = (
        "Hi - Alan here.\n\n"
        f"Thanks for sending details for {agency}.\n\n"
        "Based on this request, I do not think this is the right fit yet. "
        "This is strongest when the buyer already has one unanswered quote, last reply, rough draft, or a few bullets "
        "and wants one next email to send.\n\n"
        "Here is an example output:\n\n"
        f"{sample_text}\n\n"
        "Reply with one unanswered quote, last reply, rough draft, or a few bullets and I can send one follow-up email before any payment.\n\n"
        "- Alan"
    )
    html_body = (
        f"<p>Hi - Alan here.</p>"
        f"<p>Thanks for sending details for {escape(agency)}.</p>"
        "<p>Based on this request, I do not think this is the right fit yet. "
        "This is strongest when the buyer has one quiet prospect, a real last reply or rough draft, "
        "and a need for follow-up material that moves the deal forward.</p>"
        "<p>Here is an example output:</p>"
        f"<p>{sample_html}</p>"
        "<p>Reply with one unanswered quote, last reply, rough draft, or a few bullets and I can send one follow-up email before any payment.</p>"
        "<p>- Alan</p>"
    )
    return subject, text_body, html_body


def next_action_for_route(route: str) -> str:
    if route == "pilot_path":
        return "Ask for one unanswered quote and send one follow-up email before payment."
    if route == "clarify_path":
        return "Ask for one unanswered quote before recommending the follow-up email."
    return "Ask for a concrete follow-up draft or soft-disqualify."


def founder_digest_line(intake: BuyerIntake, fit_band: str, route: str, friction: str) -> str:
    agency = intake.agency_name or "unknown buyer"
    calls = "unknown calls/week" if intake.calls_per_week is None else f"{intake.calls_per_week} calls/week"
    return f"{agency} | {fit_band} | {route} | {calls} | friction: {friction}"


def build_lead_record(
    intake: BuyerIntake,
    fit_band: str,
    route: str,
    score: int,
    confidence: str,
    friction: str,
    next_action: str,
) -> Dict[str, Any]:
    return {
        "work_email": intake.work_email,
        "agency_name": intake.agency_name,
        "website": intake.website,
        "calls_per_week": intake.calls_per_week,
        "biggest_post_call_bottleneck": intake.biggest_post_call_bottleneck,
        "recent_call_notes": intake.recent_call_notes,
        "fit_band": fit_band,
        "route": route,
        "fit_score": score,
        "confidence": confidence,
        "main_friction": friction,
        "next_action": next_action,
    }


def run_buyer_engine_v1(payload: Dict[str, Any], sample_packet_url: str) -> BuyerDecision:
    intake = from_form_payload(payload)
    fit_band, route, confidence, score, reasons, friction = classify_fit(intake)
    subject, text_body, html_body = render_buyer_reply(
        intake=intake,
        fit_band=fit_band,
        route=route,
        friction=friction,
        sample_packet_url=sample_packet_url,
    )
    next_action = next_action_for_route(route)
    digest = founder_digest_line(intake, fit_band, route, friction)
    lead_record = build_lead_record(
        intake=intake,
        fit_band=fit_band,
        route=route,
        score=score,
        confidence=confidence,
        friction=friction,
        next_action=next_action,
    )
    return BuyerDecision(
        fit_band=fit_band,
        route=route,
        score=score,
        confidence=confidence,
        main_friction=friction,
        reasons=reasons,
        next_action=next_action,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        founder_digest=digest,
        lead_record=lead_record,
    )


def buyer_decision_to_dict(decision: BuyerDecision) -> Dict[str, Any]:
    return asdict(decision)
