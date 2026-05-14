import os

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.api.routes.relay_intent import (
    _autonomous_money_mandate,
    _money_decision_contract,
    _owner_absence_contract,
    _success_governor_contract,
)


def test_completed_sample_waits_during_reply_observation_window():
    money_decision = _money_decision_contract(
        active_sends=10,
        active_target=10,
        active_remaining=0,
        active_signal_replies=0,
        active_signal_payments=0,
        auto_closed_replies=0,
        unhandled_replies=0,
        checkout_clicks=0,
        payments=0,
        experiment_decision_next="send the active sample",
        active_reply_observation_pending=True,
        active_reply_observation_deadline="2026-05-15T13:30:12+00:00",
    )

    assert money_decision["state"] == "observe_reply_window"
    assert "Wait until 2026-05-15T13:30:12+00:00" in money_decision["next_action"]


def test_success_governor_does_not_rotate_before_reply_window_deadline():
    revenue_objective = {"state": "below_target", "current_week_payments": 0, "current_week_gross_usd": 0}
    money_decision = {
        "state": "observe_reply_window",
        "next_action": "Wait until 2026-05-15T13:30:12+00:00 before changing copy or targeting.",
    }
    reply_autonomy = {"state": "idle"}
    launch_readiness = {"ready": True, "next_window_audit_at": "2026-05-15T13:30:12+00:00"}
    operator_mode = {"do_not_interrupt_user": True}

    governor = _success_governor_contract(
        revenue_objective=revenue_objective,
        money_decision=money_decision,
        reply_autonomy=reply_autonomy,
        launch_readiness=launch_readiness,
        operator_mode=operator_mode,
    )
    owner_absence = _owner_absence_contract(
        success_governor=governor,
        launch_readiness=launch_readiness,
        reply_autonomy=reply_autonomy,
        conversion_ladder={"active_stage": "review"},
        revenue_objective=revenue_objective,
    )
    mandate = _autonomous_money_mandate(
        revenue_objective=revenue_objective,
        money_decision=money_decision,
        reply_autonomy=reply_autonomy,
        conversion_ladder={"active_stage": "review"},
        success_governor=governor,
        owner_absence=owner_absence,
        launch_readiness={"active_experiment_sends_remaining": 0, "expected_next_window_sends": 0},
    )

    assert governor["state"] == "observe_reply_window"
    assert owner_absence["permitted_action"] == "observe the active sample until the deadline before rotating one variable"
    assert mandate["state"] == "observe_reply_window"

