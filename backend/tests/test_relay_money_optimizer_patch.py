import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.services.relay_money_optimizer_patch import _direct_fill_candidates_after_active_first


def _candidate(external_id: str, variant: str):
    return (SimpleNamespace(external_id=external_id), object(), variant, False)


def test_direct_fill_candidates_run_after_active_first_touches():
    active_variant = "hard_paid_test_direct"
    active_candidate = _candidate("active-1", active_variant)
    direct_followup = _candidate("direct-followup-1", "paid_test_explicit")
    second_followup = _candidate("direct-followup-2", "stalled_opportunity_direct")
    same_variant_non_sample = _candidate("same-active-variant", active_variant)

    fill = _direct_fill_candidates_after_active_first(
        [active_candidate, direct_followup, second_followup, same_variant_non_sample],
        active_sample_ids={"active-1"},
        active_variant=active_variant,
        fill_slots=2,
        active_sample_can_complete_now=True,
    )

    assert fill == [direct_followup, second_followup]


def test_direct_fill_candidates_keep_reserved_slots_empty_when_no_capacity():
    fill = _direct_fill_candidates_after_active_first(
        [_candidate("direct-followup-1", "paid_test_explicit")],
        active_sample_ids=set(),
        active_variant="hard_paid_test_direct",
        fill_slots=0,
        active_sample_can_complete_now=True,
    )

    assert fill == []


def test_direct_fill_candidates_do_not_starve_incomplete_active_sample():
    fill = _direct_fill_candidates_after_active_first(
        [_candidate("direct-followup-1", "paid_test_explicit")],
        active_sample_ids={"active-1"},
        active_variant="hard_paid_test_direct",
        fill_slots=8,
        active_sample_can_complete_now=False,
    )

    assert fill == []
