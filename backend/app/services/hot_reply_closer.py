from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.core.config import entry_price_label, settings


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
    return (
        f"one follow-up rewrite: preview first, {entry_price_label()} only after it helps\n"
        "larger cleanup: Alan replies manually after the first useful preview"
    )


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
                "send one rough follow-up first and i will reply with the preview before any payment"
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
                "send one rough follow-up first. preview first; payment link afterward if you use it"
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
                "send one rough follow-up first. i will reply with the preview before any payment."
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
                "lowest-friction option is one rough follow-up. send the draft or a few bullets first; i will reply with the preview before any payment."
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
