import asyncio
import os
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.core.config import relay_costs_paused, relay_paid_fulfillment_allowed_when_paused
from app.api.routes.relay_intent import _send_sample_email
from app.services import autonomous_ops
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


def test_relay_costs_paused_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_COSTS_PAUSED", raising=False)
    monkeypatch.delenv("AO_RELAY_MONEY_LOOP_PAUSED", raising=False)

    assert relay_costs_paused() is True


def test_relay_costs_pause_requires_explicit_false(monkeypatch):
    monkeypatch.setenv("AO_RELAY_COSTS_PAUSED", "false")

    assert relay_costs_paused() is False


def test_paid_fulfillment_bypass_is_enabled_by_default(monkeypatch):
    monkeypatch.delenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", raising=False)

    assert relay_paid_fulfillment_allowed_when_paused() is True


def test_paid_fulfillment_bypass_can_be_disabled(monkeypatch):
    monkeypatch.setenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "false")

    assert relay_paid_fulfillment_allowed_when_paused() is False


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


def test_money_summary_classifies_current_entry_price(monkeypatch):
    monkeypatch.setenv("RELAY_FIRST_MONEY_PRICE_USD", "1")

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
    monkeypatch.setenv("AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED", "false")

    assert send_due_sequence_messages()["status"] == "paused"
    assert poll_reply_mailbox()["status"] == "paused"
    assert send_paid_onboarding_for_email("buyer@example.com")["status"] == "paused"
    assert process_tally_submission({})["status"] == "paused"
    assert _send_sample_email("buyer@example.com")["status"] == "paused"
