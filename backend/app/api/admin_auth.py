from __future__ import annotations

from secrets import compare_digest

from fastapi import Header, HTTPException

from app.core.config import settings


def require_relay_admin(x_relay_admin: str | None = Header(default=None)) -> None:
    expected = (settings.ops_admin_token or "").strip()
    supplied = (x_relay_admin or "").strip()
    if not expected or not supplied or not compare_digest(supplied, expected):
        raise HTTPException(status_code=404, detail="not found")
