# Cattle Movement Forecasting System: Activation Guide

## 1) Open terminal in backend folder

```bash
cd "/Users/nikhilavin/Desktop/HTML Frontend UN Challenge/backend"
```

## 2) Create env file

```bash
cp .env.example .env
```

## 3) Add your Google Earth key

Edit:

`/Users/nikhilavin/Desktop/HTML Frontend UN Challenge/backend/.env`

Set:

```env
GOOGLE_EARTH_API_KEY=YOUR_REAL_KEY_HERE
```

Optional (for live Earth Engine probe calls):

```env
ENABLE_LIVE_EARTH_CALLS=true
```

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

## Common issues

- `ENOTFOUND registry.npmjs.org` during `npm install`
  - Your network/DNS is blocking npm access. Fix network access, then rerun `npm install`.

- Frontend says "Not Connected"
  - Confirm backend is running on port `8080`.
  - Open the forced URL with `?api=http://localhost:8080`.

