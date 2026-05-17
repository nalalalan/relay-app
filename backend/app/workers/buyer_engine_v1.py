from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from app.services.buyer_engine_v1 import buyer_decision_to_dict, run_buyer_engine_v1


DEFAULT_SAMPLE_PACKET_URL = "https://relaybrief.com/sample.pdf"


def process_buyer_submissions(
    submissions: Iterable[Dict[str, Any]],
    sample_packet_url: str = DEFAULT_SAMPLE_PACKET_URL,
) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    for submission in submissions:
        decision = run_buyer_engine_v1(submission, sample_packet_url=sample_packet_url)
        outputs.append(buyer_decision_to_dict(decision))
    return outputs


def write_founder_digest(
    outputs: List[Dict[str, Any]],
    target_path: str | Path,
) -> Path:
    path = Path(target_path)
    lines = ["Buyer Engine v1 founder digest", ""]
    for item in outputs:
        lines.append(f"- {item['founder_digest']}")
        lines.append(f"  next: {item['next_action']}")
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path
