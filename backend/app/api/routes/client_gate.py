from __future__ import annotations

from fastapi import APIRouter, Request
from json import JSONDecodeError

from app.services.client_gate_v1 import redeem_client_access_code

router = APIRouter()


@router.post("/redeem")
async def redeem(request: Request) -> dict:
    try:
        payload = await request.json()
    except JSONDecodeError:
        return {"status": "error", "message": "invalid access request", "client_form_url": "", "client_label": ""}
    if not isinstance(payload, dict):
        return {"status": "error", "message": "invalid access request", "client_form_url": "", "client_label": ""}
    code = str(payload.get("access_code", "")).strip()
    result = redeem_client_access_code(code)
    return result.__dict__
