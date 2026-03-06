# Cattle Movement Forecasting System: Activation Guide

## 1) Open terminal in backend folder

```bash
cd "/Users/nikhilavin/Desktop/HTML Frontend UN Challenge/backend"
```

## 2) Create env file

```bash
cp .env.example .env
```

## 3) Configure Google Earth mode (optional key)

Edit:

`/Users/nikhilavin/Desktop/HTML Frontend UN Challenge/backend/.env`

If you have a key, set:

```env
GOOGLE_EARTH_API_KEY=YOUR_REAL_KEY_HERE
```

Enable live probes only when you want real Earth connectivity checks:

```env
ENABLE_LIVE_EARTH_CALLS=true
```

If key is missing (or live probes are disabled), backend automatically uses fallback mode and still returns full API outputs.

## 4) Install and start backend

```bash
npm install
npm start
```

## 5) Open the frontend

Open in browser:

`http://localhost:8080/prototype`

If needed, force API base:

`http://localhost:8080/prototype?api=http://localhost:8080`

## 6) Verify backend is running

Health:

`http://localhost:8080/health`

Google Earth provider status:

`http://localhost:8080/providers/google-earth/status`

Force an immediate re-check (bypass cache):

`http://localhost:8080/providers/google-earth/status?force=true`

Multi-source provider status (active provider + fallback chain):

`http://localhost:8080/providers/status`

Expected fallback response without a key:

- `status: "missing_api_key_fallback"`
- `runtimeMode: "fallback"`

## Common issues

- `ENOTFOUND registry.npmjs.org` during `npm install`
  - Your network/DNS is blocking npm access. Fix network access, then rerun `npm install`.

- Frontend says "Not Connected"
  - Confirm backend is running on port `8080`.
  - Open the forced URL with `?api=http://localhost:8080`.
