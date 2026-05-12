from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

from app.core.config import entry_checkout_url, entry_price_label, settings


Intent = Literal[
    "negative",
    "pricing_single",
    "pricing_multi",
    "trust_objection",
    "timing_objection",
    "scope_question",
    "call_request",
    "hot_high_ticket",
    "manual_review",
]


@dataclass
class HotReplyDecision:
    intent: Intent
    heat_score: int
    offer_tier: str
    should_alert: bool
    should_stop_sequence: bool
    reply_text: str
    summary: str


def _ladder_block() -> str:
    packet = entry_checkout_url()
    offers = [(f"one live packet ({entry_price_label()})", packet)]
    seen = {packet}
    candidates = [
        (
            "5-call sprint ($750)",
            os.getenv("PACKET_5_PACK_URL", "").strip() or getattr(settings, "packet_5_pack_url", ""),
        ),
        (
            "done-for-you week ($3000)",
            os.getenv("WEEKLY_SPRINT_URL", "").strip() or getattr(settings, "weekly_sprint_url", ""),
        ),
        (
            "done-for-you month ($7500)",
            os.getenv("MONTHLY_AUTOPILOT_URL", "").strip() or getattr(settings, "monthly_autopilot_url", ""),
        ),
    ]
    for label, url in candidates:
        if url and url not in seen:
            offers.append((label, url))
            seen.add(url)
    return "\n\n".join(f"{label}:\n{url}" for label, url in offers)


def build_hot_reply_decision(text: str) -> HotReplyDecision:
    t = (text or "").strip().lower()

    if any(x in t for x in ["unsubscribe", "remove me", "stop", "not interested", "leave me alone"]):
        return HotReplyDecision(
            intent="negative",
            heat_score=0,
            offer_tier="none",
            should_alert=False,
            should_stop_sequence=True,
            reply_text="",
            summary="negative reply",
        )

    if any(x in t for x in ["call", "talk", "zoom", "phone", "meeting"]):
        return HotReplyDecision(
            intent="call_request",
            heat_score=95,
            offer_tier="high_ticket",
            should_alert=True,
            should_stop_sequence=True,
            reply_text=(
                "happy to keep this simple\n\n"
                f"{_ladder_block()}\n\n"
                "if the week or month is the real fit, start there and intake opens automatically after checkout"
            ),
            summary="call request / high intent",
        )

    if any(x in t for x in ["monthly", "retainer", "ongoing", "multiple calls", "for clients", "for the team", "every week", "weekly"]):
        return HotReplyDecision(
            intent="hot_high_ticket",
            heat_score=100,
            offer_tier="week_or_month",
            should_alert=True,
            should_stop_sequence=True,
            reply_text=(
                "for multiple calls, i would skip the one-off and go straight to the bigger fit\n\n"
                f"{_ladder_block()}\n\n"
                "if this is already a recurring pain point, the week or month is the better move"
            ),
            summary="high-ticket buying signal",
        )

    if any(x in t for x in ["how much", "price", "pricing", "cost", "$"]):
        return HotReplyDecision(
            intent="pricing_single",
            heat_score=70,
            offer_tier="ladder",
            should_alert=False,
            should_stop_sequence=True,
            reply_text=(
                "totally - pick whichever is the best fit\n\n"
                f"{_ladder_block()}\n\n"
                "after checkout, intake opens automatically"
            ),
            summary="pricing question",
        )

    if any(x in t for x in ["sample", "proof", "scam", "what exactly", "what do i get", "how does it work"]):
        return HotReplyDecision(
            intent="trust_objection",
            heat_score=55,
            offer_tier="proof",
            should_alert=False,
            should_stop_sequence=True,
            reply_text=(
                f"totally fair\n\nsite + sample:\n{settings.landing_page_url}\n\n"
                f"lowest-risk test:\n{entry_checkout_url()}\n\n"
                "if you already know the workflow is useful, the sprint/week options are the better fit"
            ),
            summary="trust / scope objection",
        )

    if any(x in t for x in ["later", "not now", "busy", "circle back", "next week"]):
        return HotReplyDecision(
            intent="timing_objection",
            heat_score=40,
            offer_tier="single_or_week",
            should_alert=False,
            should_stop_sequence=True,
            reply_text=(
                "makes sense\n\n"
                f"lowest-friction option is one real call:\n{entry_checkout_url()}\n\n"
                "if you want this off your plate for the week instead, the week option is probably the better fit"
            ),
            summary="timing objection",
        )

    return HotReplyDecision(
        intent="manual_review",
        heat_score=35,
        offer_tier="manual",
        should_alert=True,
        should_stop_sequence=True,
        reply_text="",
        summary="manual review",
    )
