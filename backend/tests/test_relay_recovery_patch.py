import os

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.services.relay_recovery_patch import _apollo_fallback_refill_outcome


def test_apollo_fallback_refill_outcome_errors_when_fallback_adds_no_capacity():
    outcome = _apollo_fallback_refill_outcome(
        {"status": "error", "error_type": "ApifyApiError"},
        [
            {
                "status": "error",
                "reason": "apify_fallback_failed",
                "refill_capacity_delta": 0,
            }
        ],
    )

    assert outcome == {
        "status": "error",
        "reason": "apollo_refill_failed_apify_fallback_no_capacity",
    }


def test_apollo_fallback_refill_outcome_is_degraded_ok_only_with_capacity():
    outcome = _apollo_fallback_refill_outcome(
        {"status": "ok", "refill_capacity_delta": 2},
        [{"status": "ok", "refill_capacity_delta": 2}],
    )

    assert outcome == {
        "status": "degraded_ok",
        "reason": "apollo_refill_failed_apify_fallback_created_capacity",
    }


def test_apollo_fallback_refill_outcome_errors_without_fallback_attempts():
    outcome = _apollo_fallback_refill_outcome({}, [])

    assert outcome == {
        "status": "error",
        "reason": "apollo_refill_failed_apify_fallback_failed",
    }
