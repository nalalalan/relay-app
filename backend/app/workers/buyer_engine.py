from __future__ import annotations

from app.schemas.revenue_ops import BuyerRequestIn
from app.services.buyer_fit import score_buyer_fit


def draft_buyer_engine_row(buyer: BuyerRequestIn) -> dict:
    fit = score_buyer_fit(
        agency_name=buyer.agency_name,
        website=buyer.website,
        calls_per_week=buyer.calls_per_week,
        bottleneck=buyer.bottleneck,
    )

    first_message = (
        "Saw that your team is handling sales calls and the post-call follow-up still carries founder/admin drag. "
        "RelayBrief turns rough notes from one sales or client call into one clean follow-up email."
    )

    return {
        "agency_name": buyer.agency_name,
        "website": buyer.website,
        "email": buyer.email,
        "fit_band": fit.fit_band,
        "fit_score": fit.score,
        "fit_reason": fit.reason,
        "first_outreach_draft": first_message,
        "status": "ready_for_review" if fit.score >= 30 else "low_priority",
    }
