import asyncio
import os
from pathlib import Path
from datetime import datetime, timezone
from json import JSONDecodeError

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("AO_RELAY_FULLY_PAUSED", "false")

from app.core.config import (
    entry_checkout_url,
    relay_costs_paused,
    relay_fully_paused,
    relay_inbound_contact_allowed_when_paused,
    relay_paid_fulfillment_allowed_when_paused,
)
from app.api.routes.relay_intent import _send_messy_notes_customer_email, _send_sample_email
from app.api.routes import client_gate
from app.services import autonomous_ops, relay_recovery_patch
from app.services.autonomous_ops import run_autonomous_cycle
from app.services.custom_outreach import poll_reply_mailbox, send_due_sequence_messages
from app.services.post_purchase_autopilot import send_paid_onboarding_for_email
from app.services import post_purchase_autopilot
from app.workers import fulfillment
from app.services.relay_recovery_patch import (
    _relay_money_loop_tick_with_timeout,
    _money_loop_state,
    start_relay_money_loop,
)
from app.workers.fulfillment import process_tally_submission


class _GateRequest:
    def __init__(self, payload=None, *, bad_json: bool = False):
        self.payload = payload
        self.bad_json = bad_json

    async def json(self):
        if self.bad_json:
            raise JSONDecodeError("bad json", "{", 1)
        return self.payload


def test_relay_costs_paused_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    assert relay_costs_paused() is True


def test_relay_costs_pause_requires_explicit_false(monkeypatch):
    monkeypatch.setenv("AO_RELAY_COSTS_PAUSED", "false")

    assert relay_costs_paused() is False


def test_full_pause_is_explicit_and_closes_purchase_and_bypass_paths(monkeypatch):
    monkeypatch.setenv("AO_RELAY_FULLY_PAUSED", "true")
    monkeypatch.setenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "true")
    monkeypatch.setenv("AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED", "true")

    assert relay_fully_paused() is True
    assert entry_checkout_url() == ""
    assert relay_paid_fulfillment_allowed_when_paused() is False
    assert relay_inbound_contact_allowed_when_paused() is False


def test_full_pause_forces_cost_pause_even_if_cost_flag_is_false(monkeypatch):
    monkeypatch.setenv("AO_RELAY_FULLY_PAUSED", "true")
    monkeypatch.setenv("AO_RELAY_COSTS_PAUSED", "false")

    assert relay_costs_paused() is True


def test_full_pause_starts_no_background_task(monkeypatch):
    monkeypatch.setenv("AO_RELAY_FULLY_PAUSED", "true")

    async def run() -> None:
        await relay_recovery_patch.stop_relay_money_loop()
        start_relay_money_loop()

        assert _money_loop_state["status"] == "fully_paused"
        assert _money_loop_state["enabled"] is False
        assert _money_loop_state["running"] is False
        assert _money_loop_state["next_wake_reason"] == "manual_owner_restart_only"
        assert relay_recovery_patch._paid_lifecycle_task is None
        assert relay_recovery_patch._money_loop_task is None

    asyncio.run(run())


def test_full_pause_blocks_mutating_http_routes(monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app

    monkeypatch.setenv("AO_RELAY_FULLY_PAUSED", "true")

    with TestClient(app) as client:
        response = client.post("/api/relay/event", json={"event_type": "page_view"})

    assert response.status_code == 503
    assert response.json()["reason"] == "relay_fully_paused"


def test_paid_fulfillment_bypass_is_enabled_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", raising=False)

    assert relay_paid_fulfillment_allowed_when_paused() is True


def test_paid_fulfillment_bypass_can_be_disabled(monkeypatch):
    monkeypatch.setenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "false")

    assert relay_paid_fulfillment_allowed_when_paused() is False


def test_inbound_contact_bypass_is_enabled_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED", raising=False)

    assert relay_inbound_contact_allowed_when_paused() is True


def test_inbound_contact_bypass_can_be_disabled(monkeypatch):
    monkeypatch.setenv("AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED", "false")

    assert relay_inbound_contact_allowed_when_paused() is False


def test_paid_intake_blocks_include_access_code(monkeypatch):
    monkeypatch.setenv("CLIENT_GATE_CODES", "paid-code,backup-code")
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)

    blocks, included = post_purchase_autopilot._intake_access_blocks("https://relaybrief.com/client-intake.html")

    assert included is True
    assert any("Access code: paid-code" in block for block in blocks)
    assert any("paste that code" in block for block in blocks)


def test_paid_intake_blocks_explain_missing_code_fallback(monkeypatch):
    monkeypatch.delenv("CLIENT_GATE_CODES", raising=False)
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)

    blocks, included = post_purchase_autopilot._intake_access_blocks("https://relaybrief.com/client-intake.html")

    assert included is False
    assert any("reply here" in block for block in blocks)


def test_client_gate_invalid_code_returns_denied(monkeypatch):
    monkeypatch.setenv("CLIENT_GATE_CODES", "paid-code")
    monkeypatch.setenv("CLIENT_INTAKE_URL", "https://tally.so/r/example")
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)

    result = asyncio.run(client_gate.redeem(_GateRequest({"access_code": "wrong-code"})))

    assert result["status"] == "denied"
    assert result["message"] == "invalid access code"
    assert result["client_form_url"] == ""


def test_client_gate_bad_json_returns_clean_error():
    result = asyncio.run(client_gate.redeem(_GateRequest(bad_json=True)))

    assert result["status"] == "error"
    assert result["message"] == "invalid access request"
    assert result["client_form_url"] == ""


def test_client_gate_valid_code_returns_form_url(monkeypatch):
    monkeypatch.setenv("CLIENT_GATE_CODES", "paid-code")
    monkeypatch.setenv("CLIENT_INTAKE_URL", "https://tally.so/r/example")
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)

    result = asyncio.run(client_gate.redeem(_GateRequest({"access_code": "paid-code"})))

    assert result["status"] == "ok"
    assert result["client_form_url"] == "https://tally.so/r/example"


def test_client_gate_submit_bad_json_returns_clean_error():
    result = asyncio.run(client_gate.submit_paid_intake(_GateRequest(bad_json=True)))

    assert result["status"] == "error"
    assert result["message"] == "invalid access request"


def test_client_gate_submit_requires_paid_email(monkeypatch):
    monkeypatch.setenv("CLIENT_GATE_CODES", "paid-code")
    monkeypatch.setenv("CLIENT_INTAKE_URL", "https://tally.so/r/example")
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)
    monkeypatch.setattr(client_gate, "paid_customer_can_fulfill_email", lambda email: False)

    result = asyncio.run(
        client_gate.submit_paid_intake(
            _GateRequest(
                {
                    "access_code": "paid-code",
                    "email": "not-paid@example.com",
                    "client_name": "Acme",
                    "focus": "next step",
                    "tone": "direct",
                    "raw_notes": "Client went quiet after the proposal call.",
                }
            )
        )
    )

    assert result["status"] == "denied"
    assert result["message"] == "paid email not found for this intake"
    assert result["pause_reason"] == "paused_by_owner_cost_control"


def test_client_gate_submit_uses_current_paid_intake_payload(monkeypatch):
    captured = {}

    def fake_handle_intake(payload):
        captured["intake"] = payload
        return {"status": "processed", "summary": "prospect marked intake_received"}

    def fake_process_tally(payload):
        captured["fulfillment"] = payload
        return {
            "submission_id": "client-gate-test",
            "generation": {"generated_count": 1},
            "sending": {"sent_count": 1},
            "digest": {"status": "checked"},
        }

    class _StateMachine:
        def __init__(self):
            self.events = []

        def apply_event(self, event):
            self.events.append(event)

    machine = _StateMachine()
    monkeypatch.setenv("CLIENT_GATE_CODES", "paid-code")
    monkeypatch.setenv("CLIENT_INTAKE_URL", "https://tally.so/r/example")
    monkeypatch.delenv("CLIENT_GATE_CODES_JSON", raising=False)
    monkeypatch.setattr(client_gate, "paid_customer_can_fulfill_email", lambda email: True)
    monkeypatch.setattr(client_gate, "handle_intake_webhook", fake_handle_intake)
    monkeypatch.setattr(client_gate, "process_tally_submission", fake_process_tally)
    monkeypatch.setattr(client_gate, "send_intake_ack_for_email", lambda email: {"status": "sent"})
    monkeypatch.setattr(client_gate, "state_machine", machine)

    result = asyncio.run(
        client_gate.submit_paid_intake(
            _GateRequest(
                {
                    "access_code": "paid-code",
                    "email": "Buyer@Example.com",
                    "client_name": "Acme",
                    "focus": "get the next step",
                    "tone": "direct",
                    "raw_notes": "Client went quiet after the proposal call.",
                }
            )
        )
    )

    fields = captured["fulfillment"]["data"]["fields"]
    mapped = {field["label"]: field["value"] for field in fields}

    assert result["status"] == "processed"
    assert captured["intake"] == captured["fulfillment"]
    assert mapped["Where should we send your follow-up email?"] == "buyer@example.com"
    assert mapped["Client or company name"] == "Acme"
    assert mapped["What should this focus on?"] == "get the next step"
    assert mapped["Preferred tone for the follow-up email"] == "direct"
    assert mapped["Paste your stuck client email, last reply, rough draft, or bullets"] == "Client went quiet after the proposal call."
    assert len(machine.events) == 3


def test_fulfillment_reads_current_paid_intake_note_label():
    payload = {
        "data": {
            "fields": [
                {"label": "Where should we send your follow-up email?", "value": "buyer@example.com"},
                {"label": "Client or company name", "value": "Acme"},
                {"label": "What should this focus on?", "value": "get the next step"},
                {"label": "Preferred tone for the follow-up email", "value": "plain"},
                {
                    "label": "Paste your stuck client email, last reply, rough draft, or bullets",
                    "value": "Client went quiet after the proposal call.",
                },
            ]
        }
    }

    fields = fulfillment._extract_client_fields(payload)

    assert fields["raw_notes"] == "Client went quiet after the proposal call."
    assert fulfillment.FIELD_TO_HEADER["Paste your stuck client email, last reply, rough draft, or bullets"] == "raw_notes"


def test_fulfillment_delivery_subject_matches_current_product():
    assert fulfillment._delivery_subject("Acme") == "Your follow-up email - Acme"
    assert fulfillment._delivery_subject("") == "Your follow-up email"


def test_paid_conversion_copy_has_no_duplicate_price_language():
    source = Path(post_purchase_autopilot.__file__).read_text(encoding="utf-8")

    assert "entry_price_label()} $1" not in source
    assert "$1 payment is {entry_price_label()}" not in source
    assert "$1 rewrite is {entry_price_label()}" not in source


def test_post_delivery_upsell_waits_for_fulfillment_event():
    source = Path(post_purchase_autopilot.__file__).read_text(encoding="utf-8")
    upsell_block = source[source.index("def run_post_delivery_upsell_sweep") : source.index("def run_messy_notes_checkout_followup_sweep")]

    assert 'AcquisitionEvent.event_type == "autopilot_paid_relay_notes_fulfilled"' in upsell_block
    assert 'AcquisitionEvent.event_type == "intake_received"' not in upsell_block


def test_paid_lifecycle_includes_second_intake_reminder():
    autopilot_source = Path(post_purchase_autopilot.__file__).read_text(encoding="utf-8")
    ops_source = Path(autonomous_ops.__file__).read_text(encoding="utf-8")

    assert "def run_paid_intake_second_reminder_sweep" in autopilot_source
    assert "autopilot_intake_second_reminder_sent" in autopilot_source
    assert "run_paid_intake_second_reminder_sweep" in ops_source
    assert "second_reminders_result" in ops_source


def test_paid_reminder_due_time_moves_to_daytime_window(monkeypatch):
    monkeypatch.setenv("RELAY_PAID_REMINDER_TIMEZONE", "America/New_York")
    monkeypatch.setenv("RELAY_PAID_REMINDER_START_HOUR", "9")
    monkeypatch.setenv("RELAY_PAID_REMINDER_END_HOUR", "19")

    due_at = datetime(2026, 6, 30, 8, 57, 15)
    now = datetime(2026, 6, 29, 9, 20, tzinfo=timezone.utc)

    adjusted = post_purchase_autopilot.paid_reminder_effective_send_at(due_at, now)

    assert adjusted.isoformat() == "2026-06-30T13:00:00+00:00"


def test_paid_reminder_window_reports_next_daytime_start(monkeypatch):
    monkeypatch.setenv("RELAY_PAID_REMINDER_TIMEZONE", "America/New_York")
    monkeypatch.setenv("RELAY_PAID_REMINDER_START_HOUR", "9")
    monkeypatch.setenv("RELAY_PAID_REMINDER_END_HOUR", "19")

    status = post_purchase_autopilot.paid_reminder_window_status(
        datetime(2026, 6, 29, 9, 10, tzinfo=timezone.utc)
    )

    assert status["open"] is False
    assert status["next_send_window_at"] == "2026-06-29T13:00:00Z"


def test_paid_status_counts_onboarding_access_codes():
    import app.api.routes.relay_intent as relay_intent

    source = Path(relay_intent.__file__).read_text(encoding="utf-8")

    assert "onboarding_access_code_ids" in source
    assert 'payload.get("access_code_included") is True' in source
    assert "autopilot_paid_intake_access_code_sent\", set()) | onboarding_access_code_ids" in source


def test_money_summary_classifies_current_entry_price(monkeypatch):
    monkeypatch.setenv("RELAY_FIRST_MONEY_PRICE_USD", "1")
    monkeypatch.setenv("RELAY_FIRST_MONEY_CHECKOUT_URL", "https://example.test/relay-checkout")

    assert autonomous_ops._sale_bucket_for_amount(100) == "one_packet"
    assert autonomous_ops._sale_bucket_for_amount(4000) == "one_packet"
    assert autonomous_ops._sale_bucket_for_amount(15000) == "five_pack"
    assert autonomous_ops._sale_bucket_for_amount(75000) == "monthly"
    assert autonomous_ops._sale_bucket_for_amount(1234) == "unknown"


def test_money_summary_uses_configured_internal_emails(monkeypatch):
    monkeypatch.setenv("RELAY_INTERNAL_EMAILS", "alan@example.com,team@example.com")

    assert autonomous_ops._is_internal_email("team@example.com") is True
    assert autonomous_ops._is_internal_email("buyer@example.com") is False


def test_money_loop_tick_returns_paused_without_work(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    result = asyncio.run(_relay_money_loop_tick_with_timeout(force_refill=True, send_live=True))

    assert result["status"] == "paused"
    assert result["refill_result"]["status"] == "paused"
    assert result["outreach_result"]["status"] == "paused"
    assert result["send_live"] is False


def test_start_money_loop_runs_paid_lifecycle_when_paused(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    async def run() -> None:
        await relay_recovery_patch.stop_relay_money_loop()
        start_relay_money_loop()

        assert _money_loop_state["status"] == "paid_lifecycle_only"
        assert _money_loop_state["enabled"] is False
        assert _money_loop_state["running"] is False
        assert _money_loop_state["next_wake_reason"] == "paid_lifecycle_cost_pause"
        assert relay_recovery_patch._paid_lifecycle_task is not None
        assert relay_recovery_patch._paid_lifecycle_task.done() is False
        await relay_recovery_patch.stop_relay_money_loop()

    asyncio.run(run())


def test_paid_lifecycle_only_status_is_visible_in_runtime(monkeypatch):
    import app.api.routes.relay_intent as relay_intent

    previous = dict(relay_recovery_patch._money_loop_state)
    try:
        relay_recovery_patch._money_loop_state.update(
            {
                "status": "paid_lifecycle_only",
                "enabled": False,
                "running": False,
                "last_error": "",
                "next_sleep_seconds": 300,
                "next_wake_reason": "paid_lifecycle_cost_pause",
                "ticks": 1,
            }
        )

        runtime = relay_intent._current_money_loop_runtime()

        assert runtime["status"] == "paid_lifecycle_only"
        assert runtime["summary"] == "owner_cost_pause_paid_lifecycle_only"
        assert runtime["next_wake_reason"] == "paid_lifecycle_cost_pause"
    finally:
        relay_recovery_patch._money_loop_state.clear()
        relay_recovery_patch._money_loop_state.update(previous)


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
    monkeypatch.setenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "false")
    monkeypatch.setenv("AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED", "false")

    assert send_due_sequence_messages()["status"] == "paused"
    assert poll_reply_mailbox()["status"] == "paused"
    assert send_paid_onboarding_for_email("buyer@example.com")["status"] == "paused"
    assert process_tally_submission({})["status"] == "paused"
    assert _send_sample_email("buyer@example.com")["status"] == "paused"
    assert _send_messy_notes_customer_email("buyer@example.com")["status"] == "paused"


def test_inbound_contact_email_allowed_under_cost_pause(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED", raising=False)
    monkeypatch.setattr("app.api.routes.relay_intent.settings.resend_api_key", "")

    assert _send_messy_notes_customer_email("buyer@example.com")["status"] == "skipped"
