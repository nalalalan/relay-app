# Relay pause and restart

Relay entered cost containment on 2026-07-18. New requests and purchases are closed, but the backend and paid-intake route remain online temporarily because seven paid prospects are still waiting for intake. Do not set `AO_RELAY_FULLY_PAUSED=true` or scale the backend to zero until those obligations are fulfilled or refunded.

## Current containment

- `AO_RELAY_FULLY_PAUSED=false`
- `AO_RELAY_COSTS_PAUSED=true`
- `AO_RELAY_MONEY_LOOP_ENABLED=false`
- `ACQ_AUTO_SEND=false`
- Railway backend replicas: `1`
- RelayBrief public request path: paused
- Stripe: all six Relay Payment Links inactive; zero active links and zero subscriptions
- RelayBrief paid-intake route: available only for already-paid customers
- Apify schedules, tasks, webhooks, rentals, and active runs: none
- `APIFY_API_TOKEN`: removed from Railway
- Apify Starter cancellation and token revocation: pending dashboard sign-in
- Smartlead: the account's only campaign (`AO buyer acquisition v2`) is paused
- Codex Relay schedules: all paused
- Database backup: `C:\Users\phama\Documents\gpt shit\_backups\relay\20260718T192010Z\relay-20260718T192010Z.dump` (SHA-256 `A586B977ADFC81732D99059540619E0F80DE1EA4FC34C8220AA890FB6AB8A5C3`; readable 117-entry `pg_restore` manifest covering all 12 expected tables)

## Full-pause target after paid obligations close

- `AO_RELAY_FULLY_PAUSED=true`
- `AO_RELAY_COSTS_PAUSED=true`
- `AO_RELAY_MONEY_LOOP_ENABLED=false`
- `ACQ_AUTO_SEND=false`
- `AO_RELAY_ALLOW_PAID_FULFILLMENT_WHEN_PAUSED=false`
- `AO_RELAY_ALLOW_INBOUND_CONTACT_WHEN_PAUSED=false`
- Railway backend replicas: `0`
- RelayBrief Stripe Payment Links: inactive
- Apify subscription and production token: canceled or revoked at the provider

With `AO_RELAY_FULLY_PAUSED=true`, mutating HTTP routes return `503`, checkout URLs resolve to an empty value, and neither the money loop nor the paid-lifecycle loop starts.

## Restart gate

Do not restart by scaling Railway alone. In this order:

1. Review the latest backup and confirm there is a real buyer or approved test that justifies spend.
2. Restore only the provider credentials that are actually required.
3. Keep `AO_RELAY_COSTS_PAUSED=true` while starting one backend replica and verify `/health` reports `paused`.
4. Set `AO_RELAY_FULLY_PAUSED=false` only after the public purchase page and fulfillment path are ready.
5. Reactivate only the specific Stripe Payment Link being offered.
6. Leave Apify disconnected unless a bounded, owner-approved scrape is required; set a hard monthly limit before restoring its token.
7. Verify one end-to-end purchase and fulfillment result before enabling any automation.

The database and public evidence history are preservation assets. Do not delete the Railway Postgres service or its volume as part of a routine pause.
