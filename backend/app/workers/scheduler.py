
"""
Recurring jobs for the acquisition supervisor.

Suggested cadence:
- hourly or daily tick for enrichment + queueing
- webhook-driven reply / payment / intake state transitions
"""

from app.services.acquisition_supervisor import tick_supervisor
from app.core.config import relay_costs_paused, relay_paused_response
from app.services.autonomous_ops import run_paid_lifecycle_tick


async def run_scheduled_jobs() -> dict:
    if relay_costs_paused():
        return {
            "status": "paid_lifecycle_only",
            "summary": "cost pause active; skipped acquisition and ran paid lifecycle only",
            "pause": relay_paused_response("scheduled_jobs"),
            "paid_lifecycle": run_paid_lifecycle_tick(),
        }

    result = await tick_supervisor(send_live=False)
    return {
        "acquisition_supervisor": "ok",
        "summary": result.summary,
        "queued": result.queued,
        "enriched": result.enriched,
        "sent_live": result.sent_live,
    }
