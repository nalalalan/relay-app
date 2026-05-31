import asyncio
import os

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.core.config import relay_costs_paused
from app.api.routes.relay_intent import _send_sample_email
from app.services.autonomous_ops import run_autonomous_cycle
from app.services.custom_outreach import poll_reply_mailbox, send_due_sequence_messages
from app.services.post_purchase_autopilot import send_paid_onboarding_for_email
from app.services.relay_recovery_patch import (
    _relay_money_loop_tick_with_timeout,
    _money_loop_state,
    start_relay_money_loop,
)
from app.workers.fulfillment import process_tally_submission


def test_relay_costs_paused_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    assert relay_costs_paused() is True


def test_relay_costs_pause_requires_explicit_false(monkeypatch):
    monkeypatch.setenv("AO_RELAY_COSTS_PAUSED", "false")

    assert relay_costs_paused() is False


def test_money_loop_tick_returns_paused_without_work(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    result = asyncio.run(_relay_money_loop_tick_with_timeout(force_refill=True, send_live=True))

    assert result["status"] == "paused"
    assert result["refill_result"]["status"] == "paused"
    assert result["outreach_result"]["status"] == "paused"
    assert result["send_live"] is False


def test_start_money_loop_does_not_start_when_paused(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    start_relay_money_loop()

    assert _money_loop_state["status"] == "paused"
    assert _money_loop_state["enabled"] is False
    assert _money_loop_state["running"] is False


def test_autonomous_cycle_paused_without_provider_calls(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    result = asyncio.run(run_autonomous_cycle(send_live=True, notify=True))

    assert result["status"] == "paused"
    assert result["search_result"]["status"] == "paused"
    assert result["outreach_result"]["status"] == "paused"
    assert result["send_live"] is False


def test_outbound_and_fulfillment_paths_return_paused(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    assert send_due_sequence_messages()["status"] == "paused"
    assert poll_reply_mailbox()["status"] == "paused"
    assert send_paid_onboarding_for_email("buyer@example.com")["status"] == "paused"
    assert process_tally_submission({})["status"] == "paused"
    assert _send_sample_email("buyer@example.com")["status"] == "paused"
