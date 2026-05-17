
from __future__ import annotations

import re


def scrub_placeholders(text: str) -> str:
    cleaned = text or ""
    cleaned = cleaned.replace("[Your Name]", "RelayBrief")
    cleaned = cleaned.replace("[your name]", "RelayBrief")
    cleaned = cleaned.replace("[name if available]", "there")
    cleaned = cleaned.replace("[Name if available]", "there")
    cleaned = re.sub(r"\[[^\]]+\]", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week.", "A follow-up and proposal outline should go out this week.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week", "A follow-up and proposal outline should go out this week.")
    return cleaned.strip()


def _trim_line_noise(line: str) -> str:
    cleaned = re.sub(r"[.]{2,}", ".", line)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.rstrip()


def _rewrite_known_awkward_line(line: str) -> str:
    stripped = line.strip().rstrip(".")
    lowered = stripped.lower()

    direct_rewrites = {
        "- scope of the short audit is not defined": "- The short-audit scope still needs to be defined.",
        "- budget for the audit is unknown": "- The audit budget still needs to be confirmed.",
        "- decision maker / primary contact is unknown": "- The primary contact and decision-maker still need to be confirmed.",
        "- decision maker and primary contact is unknown": "- The primary contact and decision-maker still need to be confirmed.",
        "- no primary contact name is listed": "- Primary contact is still unconfirmed.",
        "- no primary contact is listed": "- Primary contact is still unconfirmed.",
        "- follow-up and next step need to be sent this week": "- A follow-up should go out this week.",
        "- follow-up needs to go out this week": "- A follow-up should go out this week.",
    }
    if lowered in direct_rewrites:
        return direct_rewrites[lowered]

    return line


def _rewrite_unknown_bullet(line: str) -> str:
    stripped = line.strip()
    if not stripped.startswith("- "):
        return line

    line = _rewrite_known_awkward_line(line)
    stripped = line.strip()
    body = stripped[2:].rstrip(".")
    lowered = body.lower().strip()

    replacements = [
        ("unknown who ", "Confirm who "),
        ("unknown whether ", "Clarify whether "),
        ("unknown what ", "Clarify what "),
        ("unknown when ", "Confirm when "),
        ("unknown the exact ", "Confirm the exact "),
        ("unknown the ", "Confirm the "),
    ]
    for old, new in replacements:
        if lowered.startswith(old):
            rewritten = new + body[len(old):]
            return "- " + rewritten.rstrip(".") + "."

    return line


def reduce_unknown_clutter(text: str) -> str:
    lines = []
    for raw_line in (text or "").splitlines():
        line = _trim_line_noise(raw_line)
        norm = line.strip().lower()

        if not norm:
            lines.append(line)
            continue

        if norm in {"unknown", "- unknown"}:
            continue

        if norm.startswith("primary contact: unknown"):
            continue
        if norm.startswith("deadline: unknown"):
            continue
        if norm.startswith("future expansion opportunities: larger retainer"):
            continue

        line = _rewrite_unknown_bullet(line)
        lines.append(line)

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week.", "A follow-up and proposal outline should go out this week.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week", "A follow-up and proposal outline should go out this week.")
    return cleaned.strip()


def _summary_word_count(lines: list[str]) -> int:
    summary_words = 0
    in_summary = False
    for line in lines:
        stripped = line.strip().lower()
        if stripped == "call summary":
            in_summary = True
            continue
        if in_summary and not stripped:
            break
        if in_summary:
            summary_words += len(re.findall(r"[a-zA-Z0-9']+", line))
    return summary_words


def _bullet_priority(line: str) -> int:
    lowered = line.lower()
    if "primary contact" in lowered or "decision-maker" in lowered or "decision maker" in lowered:
        return 100
    if "budget" in lowered:
        return 90
    if "scope" in lowered:
        return 80
    if "timeline" in lowered or "deadline" in lowered:
        return 70
    if "follow-up" in lowered or "next step" in lowered:
        return 40
    return 10


def _compress_open_questions(lines: list[str], start_index: int, max_bullets: int) -> tuple[list[str], int]:
    output = [lines[start_index]]
    i = start_index + 1
    bullets = []
    while i < len(lines) and lines[i].strip().startswith("- "):
        bullets.append(lines[i])
        i += 1

    indexed = list(enumerate(bullets))
    indexed.sort(key=lambda pair: (-_bullet_priority(pair[1]), pair[0]))
    keep = indexed[:max_bullets]
    keep.sort(key=lambda pair: pair[0])

    output.extend([bullet for _, bullet in keep])
    return output, i


def _compress_internal_block(lines: list[str], start_index: int, sparse_mode: bool) -> tuple[list[str], int]:
    keep_prefixes = {"client:", "status:", "recommended first scope:", "missing info:"}
    if not sparse_mode:
        keep_prefixes |= {"deadline:"}

    output = [lines[start_index]]
    i = start_index + 1
    while i < len(lines) and lines[i].strip():
        lowered = lines[i].strip().lower()
        if any(lowered.startswith(prefix) for prefix in keep_prefixes):
            output.append(lines[i])
        i += 1
    return output, i


def _polish_global_phrasing(text: str) -> str:
    replacements = {
        "Decision maker / primary contact is unknown.": "The primary contact and decision-maker still need to be confirmed.",
        "Decision maker and primary contact is unknown.": "The primary contact and decision-maker still need to be confirmed.",
        "Follow-up and next step need to be sent this week.": "A follow-up should go out this week.",
        "Follow-up needs to go out this week.": "A follow-up should go out this week.",
        "No primary contact name is listed.": "Primary contact is still unconfirmed.",
        "No primary contact is listed.": "Primary contact is still unconfirmed.",
        "Open Questions / Risks": "Open Questions / Risks",
        "Open questions / risks": "Open Questions / Risks",
        "Internal CRM / Task Update Block": "Internal CRM / Task Update",
        "Internal CRM / task update": "Internal CRM / Task Update",
        "The next step needs to be sent this week.": "A concrete next step should go out this week.",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _compress_sparse_sections(text: str) -> str:
    lines = text.splitlines()
    sparse_mode = _summary_word_count(lines) <= 70

    output = []
    i = 0
    while i < len(lines):
        stripped = lines[i].strip().lower()

        if stripped == "open questions / risks":
            block, next_i = _compress_open_questions(lines, i, max_bullets=3 if sparse_mode else 4)
            output.extend(block)
            i = next_i
            continue

        if stripped == "internal crm / task update block":
            block, next_i = _compress_internal_block(lines, i, sparse_mode=sparse_mode)
            output.extend(block)
            i = next_i
            continue

        output.append(lines[i])
        i += 1

    return "\n".join(output)


def clean_packet_text(text: str) -> str:
    cleaned = scrub_placeholders(text)
    cleaned = reduce_unknown_clutter(cleaned)
    cleaned = _compress_sparse_sections(cleaned)
    cleaned = _polish_global_phrasing(cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r"[.]{2,}", ".", cleaned)
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the next step on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads.", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Focus the engagement on improving lead quality from Google Ads", "Focus the next step on diagnosing and improving lead quality from Google Ads.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week.", "A follow-up and proposal outline should go out this week.")
    cleaned = cleaned.replace("Follow-up and next-step proposal should go out this week", "A follow-up and proposal outline should go out this week.")
    return cleaned.strip()
