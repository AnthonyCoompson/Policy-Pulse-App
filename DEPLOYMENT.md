# PolicyPulse — Deployment Guide

## Architecture

```
Netlify (index.html)  ──API calls──►  Railway.app (Python backend)
                                              │
                                         SQLite DB
                                         (persists forever)
                                              │
                                    Scrapes 20 gov sources daily
                                    AI-tags via Gemini API
```

---

## Step 1 — Deploy Backend to Railway.app (15 minutes)

### 1a. Create a GitHub repository

1. Go to https://github.com/new
2. Create a repo called `policypulse-backend`
3. Upload all files from the `policypulse-backend/` folder:
   - `main.py`
   - `database.py`
   - `scraper.py`
   - `ai_processor.py`
   - `scheduler.py`
   - `requirements.txt`
   - `railway.toml`

### 1b. Deploy on Railway

1. Go to https://railway.app and sign in with GitHub
2. Click **New Project → Deploy from GitHub repo**
3. Select your `policypulse-backend` repo
4. Railway auto-detects Python and builds it

### 1c. Set Environment Variables

In Railway → your project → **Variables**, add:

| Variable | Value |
|---|---|
| `GEMINI_API_KEY` | Your Gemini API key from aistudio.google.com/apikey |
| `DB_PATH` | `policypulse.db` |

### 1d. Get your Railway URL

After deploy succeeds, click **Settings → Networking → Generate Domain**

You'll get a URL like: `https://policypulse-backend-production.up.railway.app`

Test it: open `https://your-url.railway.app/health` in your browser — you should see `{"status":"ok"}`

---

## Step 2 — Connect Frontend to Backend

1. Open your PolicyPulse Netlify app
2. You'll see a **"Backend Not Connected"** panel in the Dashboard
3. Paste your Railway URL and click **Connect**
4. The URL is saved to your browser's localStorage — you only do this once

---

## Step 3 — Run Your First Scrape

1. Click **Scrape Now** in the Dashboard or Scraper tab
2. The backend fetches all 20 sources and AI-processes each article
3. Feed refreshes automatically after ~60 seconds
4. Articles are now stored in SQLite — **they persist between sessions**

---

## Step 4 — Update Netlify Frontend

When you get a new `index.html`:
1. Go to your Netlify site dashboard
2. Click **Deploys** tab
3. Drag and drop the new `index.html`
4. Your Railway URL stays saved in localStorage — no reconnection needed

---

## Scheduled Scraping

The backend automatically scrapes all sources every day at **7:00 AM Vancouver time**.
You don't need to do anything — just open the app and your feed will already be fresh.

---

## Data Persistence

- All articles are stored in `policypulse.db` on Railway's persistent volume
- Read/unread status, staged articles, and digest history all persist between sessions
- Articles are deduplicated by URL — no duplicates even if the same source is scraped twice

---

## Sources (20 total)

| Source | Jurisdiction |
|---|---|
| BC Ministry of Post-Secondary Education | BC |
| Government of Canada — Education | Federal |
| BC Legislature | BC |
| BC Indigenous Relations & Reconciliation | BC |
| University Affairs Canada | Federal |
| Burnaby City Hall | Municipal |
| Higher Education Strategy Associates | Pan-Canadian |
| Innovation, Science and Economic Development Canada | Federal |
| BC Government Newsroom | BC |
| SSHRC | Federal |
| NSERC | Federal |
| CIHR | Federal |
| Universities Canada | Federal |
| First Nations Health Authority | BC |
| BC First Nations Summit | BC |
| Crown-Indigenous Relations Canada | Federal |
| Times Higher Education | International |
| Policy Options (IRPP) | Federal |
| Maclean's Education | Federal |
| BC Public Service Agency | BC |

---

## Troubleshooting

**"Scrape Started" but no new articles after 60s**
- Check Railway logs (your project → Deployments → View Logs)
- Some government sites block scrapers intermittently — this is normal
- Articles scoring below 6/10 relevance are filtered out automatically

**API errors in browser console**
- Make sure your Railway URL has no trailing slash
- Check that GEMINI_API_KEY is set in Railway Variables

**Articles not persisting after refresh**
- Confirm your Railway URL is saved (check localStorage in browser devtools)
- The backend must be running on Railway (not sleeping)

---

## Cost

| Service | Cost |
|---|---|
| Netlify (frontend) | Free |
| Railway (backend) | Free tier — $5/month after 500 hours |
| Gemini API | Free tier — generous for this use case |
| **Total** | **~$0–5/month** |
