
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.acquisition_supervisor import router as acquisition_supervisor_router
from app.api.routes.client_gate import router as client_gate_router
from app.api.routes.daily_lead_drop_runner import router as daily_lead_drop_runner_router
from app.api.routes.health import router as health_router
from app.api.routes.lead_drop_intake import router as lead_drop_intake_router
from app.api.routes.outreach_autopilot import router as outreach_autopilot_router
from app.api.routes.production_wiring import router as production_wiring_router
from app.api.routes.real_lead_source_daily_outbound import router as real_lead_source_daily_outbound_router
from app.api.routes.webhooks import router as webhook_router
from app.db.base import Base, engine
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect  # noqa: F401
from app.models.buyer_acquisition_v1 import BuyerAcquisitionMessage, BuyerAcquisitionProspect  # noqa: F401
from app.models.funnel import Lead  # noqa: F401
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead  # noqa: F401
from app.models.production_wiring import (  # noqa: F401
    ProductionAction,
    ProductionException,
    ProductionLead,
    ProductionOpportunity,
    ProductionTransition,
)
from app.api.routes.autonomous_ops import router as autonomous_ops_router
from app.api.routes.custom_outreach import router as custom_outreach_router
from app.api.routes.relay_intent import router as relay_intent_router
from app.services.relay_recovery_patch import (
    apply_relay_recovery_patch,
    router as relay_recovery_router,
)
from app.services.relay_money_optimizer_patch import apply_relay_money_optimizer_patch
from app.services.relay_reply_autoclose_patch import apply_relay_reply_autoclose_patch


apply_relay_recovery_patch()
apply_relay_money_optimizer_patch()
apply_relay_reply_autoclose_patch()


def _cors_origins() -> list[str]:
    defaults = [
        "https://relay.aolabs.io",
        "https://www.relay.aolabs.io",
        "https://liverelay.aolabs.io",
        "https://www.liverelay.aolabs.io",
        "https://nalalalan.github.io",
        "https://nalalalan.github.io/alan-operator-site",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

    raw = os.getenv("CLIENT_GATE_ALLOWED_ORIGINS", "").strip()
    configured = [x.strip() for x in raw.split(",") if x.strip()] if raw else []

    merged: list[str] = []
    for origin in configured + defaults:
        if origin and origin not in merged:
            merged.append(origin)

    return merged


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="ao-relay-backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(webhook_router, prefix="/webhooks", tags=["webhooks"])
app.include_router(production_wiring_router, prefix="/production", tags=["production"])
app.include_router(outreach_autopilot_router, prefix="/autopilot", tags=["autopilot"])
app.include_router(client_gate_router, prefix="/client-gate", tags=["client-gate"])
app.include_router(real_lead_source_daily_outbound_router, prefix="/daily-outbound", tags=["daily-outbound"])
app.include_router(lead_drop_intake_router, prefix="/lead-drop", tags=["lead-drop"])
app.include_router(daily_lead_drop_runner_router, prefix="/daily-lead-drop-runner", tags=["daily-lead-drop-runner"])
app.include_router(acquisition_supervisor_router, prefix="/acquisition-supervisor", tags=["acquisition-supervisor"])
app.include_router(relay_recovery_router, prefix="/acquisition-supervisor", tags=["acquisition-supervisor"])
app.include_router(autonomous_ops_router, prefix="/ops", tags=["ops"])
app.include_router(custom_outreach_router, prefix="/custom-outreach", tags=["custom-outreach"])
app.include_router(relay_intent_router, prefix="/api/relay", tags=["relay-intent"])


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "ok", "service": "ao-relay-backend"}
