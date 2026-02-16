# Satellite-Driven Cattle Forecasting Backend

Backend implementation for the PFD pipeline with Google Earth key integration.

## What this backend does

- Implements the 10-step flow as API operations.
- Supports 2-5 km master-grid generation.
- Produces daily feature vectors, GAM attractiveness scores, cost surfaces, and 7/14-day probabilistic corridors.
- Ingests and trust-weights community reports.
- Exposes validation metrics and UN-facing decision-support outputs.
- Uses `GOOGLE_EARTH_API_KEY` for Earth Engine request construction and connectivity checks.
- Adds operations features:
  - backtesting
  - alert engine
  - forecast change detection
  - data quality monitor
  - human review queue
  - export package generation (JSON/CSV/GeoJSON)
  - map layer API
  - role-based auth and audit trail

## Quick start

```bash
cd /Users/nikhilavin/Desktop/HTML\ Frontend\ UN\ Challenge/backend
cp .env.example .env
npm install
npm start
```

Server defaults to `http://localhost:8080`.

## Key endpoints

- Health and provider:
  - `GET /health`
  - `GET /providers/google-earth/status`
  - `GET /auth/me`
- Step 1-2 setup:
  - `POST /setup/data-sources/register`
  - `POST /setup/master-grid`
- Step 3-6 pipeline:
  - `POST /ingestion/daily`
  - `POST /gam/run`
  - `POST /forecast/run`
  - `POST /pipeline/run`
- Step 7 community:
  - `POST /community/reports`
  - `GET /community/reports?verified=true`
  - `POST /community/reports/:id/verify`
  - `GET /community/review-queue`
  - `POST /community/reviews/:id`
- Step 8-9 outputs:
  - `POST /validation/run`
  - `GET /signals/status`
  - `GET /corridors/active`
  - `GET /outputs/daily`
  - `GET /map/layers`
- Step 10 feedback:
  - `PATCH /model/weights`
- Ops and governance:
  - `GET /monitor/data-quality`
  - `GET /forecast/changes`
  - `GET /alerts`
  - `POST /alerts/:id/ack`
  - `POST /backtest/run`
  - `GET /backtests`
  - `POST /exports/daily`
  - `GET /exports/history`
  - `GET /exports/files/:filename`
  - `GET /audit/trail` (admin)

## Frontend-compatible routes

The prototype HTML expects these routes and they are implemented:

- `GET /signals/status`
- `GET /corridors/active`
- `POST /community/reports`
- `GET /community/reports?verified=true`

## Example workflow

```bash
# Full daily pipeline run
curl -X POST http://localhost:8080/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"date":"2026-02-14"}'

# Add a community report
curl -X POST http://localhost:8080/community/reports \
  -H "Content-Type: application/json" \
  -d '{"reporterId":"field-monitor-7","lat":7.32,"lng":31.44,"directionOfTravel":"north-east","grazingStatus":"scarce","waterStatus":"limited"}'

# Fetch active corridors
curl http://localhost:8080/corridors/active?horizonDays=7
```

## Notes on Google Earth key usage

- Set `GOOGLE_EARTH_API_KEY` in `.env`.
- `GET /providers/google-earth/status` reports whether key-based Earth Engine probes are configured/working.
- `ENABLE_LIVE_EARTH_CALLS=true` enables outbound Earth Engine metadata probes.
- If live calls are disabled/unavailable, the pipeline continues with deterministic fallback hints and still produces full outputs.

## Auth and audit

- If `ADMIN_API_TOKEN` and `ANALYST_API_TOKEN` are both empty, API runs in open mode.
- If tokens are set, protected endpoints require:
  - `x-api-token: <token>`
  - optional `x-actor-id: <user-or-service-id>`
- Mutating/protected actions are recorded to the audit trail.
