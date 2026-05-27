import os
from datetime import datetime

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.services.relay_performance import _experiment_start


def test_experiment_start_prefers_week_window_over_late_plan_timestamp():
    start, basis = _experiment_start(
        {
            "week_start_date": "2026-05-18",
            "created_at": "2026-05-21T13:33:20.277012",
            "logged_at": "2026-05-21T13:33:20.277550",
        },
        datetime(2026, 5, 27),
    )

    assert basis == "week_start_date"
    assert start == datetime(2026, 5, 18)
