from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from app.core.config import entry_checkout_url, settings
from pathlib import Path
import json
from typing import Any, Dict, List, Literal

from app.services.acquisition_engine_v1 import run_acquisition_engine_v1
from app.services.outreach_sender_pipeline_state_v1 import ingest_reply, send_or_queue_candidate
from app.services.proposal_audit_launcher_v1 import run_proposal_audit_launcher_v1
from app.services.close_path_v1 import decide_close_path


EventType = Literal[
    "lead_found",
    "outreach_sent",
    "reply_received",
    "proposal_sent",
    "payment_received",
    "fulfillment_sent",
]


@dataclass
class RevenueEventResult:
    event_type: str
    entity_id: str
    status: Literal["processed", "ignored", "error"]
    summary: str
    actions_created: List[Dict[str, Any]]
    exceptions_created: List[Dict[str, Any]]
    lead_state: str = ""
    close_state: str = ""
    fulfillment_state: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_store() -> Dict[str, Any]:
    return {
        "meta": {"version": "live_revenue_wiring_v1", "created_at": now_iso()},
        "leads": {},
        "opportunities": {},
        "transitions": [],
        "action_outbox": [],
        "exceptions": [],
        "processed_event_ids": [],
    }


def load_store(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return empty_store()
    return json.loads(p.read_text(encoding="utf-8"))


def save_store(path: str | Path, store: Dict[str, Any]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(store, indent=2, ensure_ascii=False), encoding="utf-8")
    return p


def _append_transition(store: Dict[str, Any], event_type: str, entity_id: str, old_state: str, new_state: str, summary: str) -> None:
    store["transitions"].append(
        {
            "timestamp": now_iso(),
            "event_type": event_type,
            "entity_id": entity_id,
            "old_state": old_state,
            "new_state": new_state,
            "summary": summary,
        }
    )


def _add_action(store: Dict[str, Any], action_type: str, entity_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    action = {
        "timestamp": now_iso(),
        "action_type": action_type,
        "entity_id": entity_id,
        "payload": payload,
    }
    store["action_outbox"].append(action)
    return action


def _add_exception(store: Dict[str, Any], exception_type: str, entity_id: str, summary: str) -> Dict[str, Any]:
    item = {
        "timestamp": now_iso(),
        "exception_type": exception_type,
        "entity_id": entity_id,
        "summary": summary,
    }
    store["exceptions"].append(item)
    return item


def _ensure_lead(store: Dict[str, Any], lead_id: str) -> Dict[str, Any]:
    if lead_id not in store["leads"]:
        store["leads"][lead_id] = {
            "lead_id": lead_id,
            "lead_state": "new",
            "pipeline_state": "new",
            "close_state": "new",
            "fulfillment_state": "not_started",
        }
    return store["leads"][lead_id]


def _opportunity_id_for(lead_id: str) -> str:
    return f"opp-{lead_id}"


def _build_outreach_candidate_from_acquisition(decision: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    state_record = decision["state_record"]
    return {
        "lead_id": state_record["lead_id"],
        "company_name": decision["company_name"],
        "contact_name": payload.get("contact_name", ""),
        "contact_email": payload.get("contact_email", ""),
        "fit_band": decision["fit_band"],
        "route": decision["route"],
        "outreach_subject": decision["outreach_subject"],
        "outreach_body": decision["outreach_body"],
        "founder_digest_line": decision["founder_digest_line"],
        "next_action": decision["next_action"],
        "source": payload.get("source", ""),
        "state": "new",
    }


def _build_opportunity_from_lead(lead_record: Dict[str, Any], reply_state: str, reply_text: str) -> Dict[str, Any]:
    return {
        "opportunity_id": _opportunity_id_for(lead_record["lead_id"]),
        "company_name": lead_record["company_name"],
        "contact_name": lead_record.get("contact_name", ""),
        "contact_email": lead_record.get("contact_email", ""),
        "fit_band": lead_record.get("fit_band", ""),
        "route": lead_record.get("route", ""),
        "vertical": lead_record.get("vertical", ""),
        "estimated_monthly_call_volume": lead_record.get("estimated_monthly_call_volume"),
        "known_pain": lead_record.get("notes", ""),
        "notes": lead_record.get("notes", ""),
        "reply_state": reply_state,
        "reply_text": reply_text,
    }


def _payment_link_for_offer(offer_name: str) -> str:
    return entry_checkout_url()


def process_event(store: Dict[str, Any], event: Dict[str, Any]) -> RevenueEventResult:
    event_id = str(event.get("event_id", "")).strip()
    event_type = str(event.get("event_type", "")).strip()
    payload = event.get("payload") or {}

    if event_id and event_id in store["processed_event_ids"]:
        return RevenueEventResult(
            event_type=event_type,
            entity_id=str(payload.get("lead_id") or payload.get("id") or ""),
            status="ignored",
            summary="duplicate event ignored",
            actions_created=[],
            exceptions_created=[],
        )

    actions_created: List[Dict[str, Any]] = []
    exceptions_created: List[Dict[str, Any]] = []

    try:
        if event_type == "lead_found":
            decision = asdict(run_acquisition_engine_v1(payload))
            lead_id = decision["lead_id"]
            lead = _ensure_lead(store, lead_id)
            old_state = lead["lead_state"]

            lead.update(
                {
                    "lead_id": lead_id,
                    "company_name": decision["company_name"],
                    "contact_name": payload.get("contact_name", ""),
                    "contact_email": payload.get("contact_email", ""),
                    "website": payload.get("website", ""),
                    "vertical": payload.get("vertical", ""),
                    "source": payload.get("source", ""),
                    "estimated_monthly_call_volume": payload.get("estimated_monthly_call_volume"),
                    "fit_band": decision["fit_band"],
                    "fit_score": decision["score"],
                    "route": decision["route"],
                    "lead_state": "qualified" if decision["fit_band"] != "discard" else "discarded",
                    "pipeline_state": "new",
                    "close_state": "new",
                    "fulfillment_state": "not_started",
                    "notes": payload.get("notes", ""),
                }
            )

            outreach_candidate = _build_outreach_candidate_from_acquisition(decision, payload)
            send_result = asdict(send_or_queue_candidate(outreach_candidate, existing_dedupe_keys=set()))
            lead["pipeline_state"] = send_result["pipeline_state"]

            if send_result["send_status"] == "sent":
                actions_created.append(
                    _add_action(
                        store,
                        "send_outreach",
                        lead_id,
                        {
                            "to": lead["contact_email"],
                            "subject": send_result["subject"],
                            "body": decision["outreach_body"],
                        },
                    )
                )
            elif send_result["send_status"] == "queued":
                actions_created.append(
                    _add_action(
                        store,
                        "nurture_queue",
                        lead_id,
                        {"reason": send_result["reason"], "company_name": lead["company_name"]},
                    )
                )

            if send_result["send_status"] == "duplicate_blocked":
                exceptions_created.append(
                    _add_exception(store, "duplicate_outreach", lead_id, "duplicate outreach blocked")
                )

            _append_transition(store, event_type, lead_id, old_state, lead["lead_state"], f"{decision['fit_band']} -> {send_result['send_status']}")

            summary = f"{lead['company_name']} classified as {decision['fit_band']} and {send_result['send_status']}"
            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary=summary,
                actions_created=actions_created,
                exceptions_created=exceptions_created,
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        elif event_type == "outreach_sent":
            lead_id = str(payload.get("lead_id", "")).strip()
            lead = _ensure_lead(store, lead_id)
            old_state = lead["pipeline_state"]
            lead["pipeline_state"] = "sent"
            _append_transition(store, event_type, lead_id, old_state, "sent", "outreach confirmed sent")
            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary="outreach marked sent",
                actions_created=[],
                exceptions_created=[],
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        elif event_type == "reply_received":
            lead_id = str(payload.get("lead_id", "")).strip()
            reply_text = str(payload.get("reply_text", "")).strip()
            lead = _ensure_lead(store, lead_id)

            transition = asdict(ingest_reply(lead_id, lead.get("pipeline_state", "sent"), reply_text))
            old_pipeline = lead["pipeline_state"]
            lead["pipeline_state"] = transition["new_state"]

            close_state_map = {
                "replied_interested": "interested",
                "replied_needs_info": "needs_info",
                "replied_not_now": "not_now",
                "replied_not_fit": "closed_lost",
            }
            old_close = lead["close_state"]
            if transition["new_state"] in close_state_map:
                lead["close_state"] = close_state_map[transition["new_state"]]

            _append_transition(store, event_type, lead_id, old_pipeline, lead["pipeline_state"], transition["reply_classification"])

            opp_payload = _build_opportunity_from_lead(
                lead_record=lead,
                reply_state=transition["new_state"],
                reply_text=reply_text,
            )
            launcher_decision = asdict(run_proposal_audit_launcher_v1(opp_payload))
            opp_id = launcher_decision["opportunity_id"]

            payment_link = _payment_link_for_offer(launcher_decision["recommended_offer"])
            close_payload = {
                **launcher_decision,
                "contact_name": lead.get("contact_name", ""),
                "contact_email": lead.get("contact_email", ""),
                "reply_state": transition["new_state"],
                "reply_text": reply_text,
                "payment_link": payment_link,
                "current_close_state": lead["close_state"],
            }
            close_decision = asdict(decide_close_path(close_payload))

            store["opportunities"][opp_id] = {
                **launcher_decision["state_record"],
                "opportunity_id": opp_id,
                "company_name": launcher_decision["company_name"],
                "launch_type": launcher_decision["launch_type"],
                "recommended_offer": launcher_decision["recommended_offer"],
                "buyer_email_subject": launcher_decision["buyer_email_subject"],
                "buyer_email_body": launcher_decision["buyer_email_body"],
                "close_route": close_decision["close_route"],
                "close_state": close_decision["next_close_state"],
            }

            lead["close_state"] = close_decision["next_close_state"]

            if close_decision["close_route"] in {"send_proposal", "send_clarify"}:
                actions_created.append(
                    _add_action(
                        store,
                        "send_close_email",
                        opp_id,
                        {
                            "to": lead.get("contact_email", ""),
                            "subject": close_decision["subject"],
                            "body": close_decision["body"],
                        },
                    )
                )

            if transition["new_state"] == "replied_needs_info":
                exceptions_created.append(
                    _add_exception(store, "needs_info", opp_id, "reply needs additional clarification before clean close")
                )

            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary=f"reply classified as {transition['reply_classification']} and routed to {close_decision['close_route']}",
                actions_created=actions_created,
                exceptions_created=exceptions_created,
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        elif event_type == "proposal_sent":
            lead_id = str(payload.get("lead_id", "")).strip()
            lead = _ensure_lead(store, lead_id)
            old_close = lead["close_state"]
            lead["close_state"] = "payment_pending"
            _append_transition(store, event_type, lead_id, old_close, "payment_pending", "proposal/paid-step email sent")
            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary="proposal marked sent and payment now pending",
                actions_created=[],
                exceptions_created=[],
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        elif event_type == "payment_received":
            lead_id = str(payload.get("lead_id", "")).strip()
            lead = _ensure_lead(store, lead_id)
            opp_id = _opportunity_id_for(lead_id)
            opp = deepcopy(store["opportunities"].get(opp_id, {}))
            if not opp:
                raise RuntimeError("payment_received event has no matching opportunity")

            close_payload = {
                "opportunity_id": opp_id,
                "company_name": lead["company_name"],
                "contact_name": lead.get("contact_name", ""),
                "contact_email": lead.get("contact_email", ""),
                "launch_type": opp.get("launch_type", ""),
                "recommended_offer": opp.get("recommended_offer", ""),
                "recommended_price_guidance": opp.get("price_guidance", ""),
                "best_next_commercial_move": opp.get("best_next_commercial_move", ""),
                "buyer_email_subject": opp.get("buyer_email_subject", ""),
                "buyer_email_body": opp.get("buyer_email_body", ""),
                "internal_operator_note": "",
                "reply_state": "payment_received",
                "reply_text": "Paid.",
                "payment_link": payload.get("payment_link", _payment_link_for_offer(opp.get("recommended_offer", ""))),
                "current_close_state": lead.get("close_state", "payment_pending"),
            }
            close_decision = asdict(decide_close_path(close_payload))
            old_close = lead["close_state"]
            lead["close_state"] = "paid"
            lead["fulfillment_state"] = close_decision["next_close_state"]
            _append_transition(store, event_type, lead_id, old_close, "paid", "payment received")

            if close_decision["fulfillment_should_trigger"]:
                actions_created.append(
                    _add_action(
                        store,
                        "queue_fulfillment",
                        opp_id,
                        {
                            "company_name": lead["company_name"],
                            "recommended_offer": opp.get("recommended_offer", ""),
                            "reason": close_decision["fulfillment_trigger_reason"],
                        },
                    )
                )
                exceptions_created.append(
                    _add_exception(store, "paid_opportunity", opp_id, f"{lead['company_name']} paid; fulfillment queued")
                )

            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary="payment received and fulfillment queued",
                actions_created=actions_created,
                exceptions_created=exceptions_created,
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        elif event_type == "fulfillment_sent":
            lead_id = str(payload.get("lead_id", "")).strip()
            lead = _ensure_lead(store, lead_id)
            old_state = lead["fulfillment_state"]
            lead["fulfillment_state"] = "sent"
            _append_transition(store, event_type, lead_id, old_state, "sent", "fulfillment delivered")
            result = RevenueEventResult(
                event_type=event_type,
                entity_id=lead_id,
                status="processed",
                summary="fulfillment marked sent",
                actions_created=[],
                exceptions_created=[],
                lead_state=lead["lead_state"],
                close_state=lead["close_state"],
                fulfillment_state=lead["fulfillment_state"],
            )

        else:
            raise RuntimeError(f"unsupported event_type: {event_type}")

        if event_id:
            store["processed_event_ids"].append(event_id)
        return result

    except Exception as exc:
        entity_id = str(payload.get("lead_id") or payload.get("id") or "")
        exceptions_created.append(_add_exception(store, "processing_error", entity_id, str(exc)))
        if event_id:
            store["processed_event_ids"].append(event_id)
        return RevenueEventResult(
            event_type=event_type,
            entity_id=entity_id,
            status="error",
            summary=str(exc),
            actions_created=actions_created,
            exceptions_created=exceptions_created,
        )
