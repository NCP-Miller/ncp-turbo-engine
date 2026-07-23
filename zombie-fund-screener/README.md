# Zombie Fund Screener

Finds aging PE funds ("zombie funds") whose GPs may be motivated sellers
of their remaining portfolio companies. Ten weighted signals score every
GP; you work the ranked list, kill the misfits, and run outreach through
the built-in lightweight CRM. Salesforce stays your system of record via
the Export/Sync page.

## How to run it

**On Streamlit Cloud (how this is deployed):**
1. In Streamlit Cloud, create a new app pointed at this repo with main
   file path `zombie-fund-screener/Today.py`.
2. In the app's Secrets, add the same `APP_PASSWORD`, `GITHUB_TOKEN`,
   `GITHUB_REPO`, and `OPENAI_API_KEY` you use for the sourcing app.
   The GitHub ones give the screener its permanent backup (Streamlit's
   disk is wiped on every redeploy — the backup brings everything back
   automatically); the OpenAI key powers the AI email drafting on the
   GP Detail page.

**On your own computer (Windows):** double-click `run_app.bat`.
It installs the requirements and opens the app in your browser.
(Mac/Linux: `bash run_app.sh`.)

## First-session walkthrough

1. **Seed GPs** — Data Manager → add firms one at a time or bulk-paste a
   list of names.
2. **Enter what you know** — open a GP in GP Detail: add fund vintages
   (drives Signal 1), portfolio companies with acquisition dates
   (Signal 4), and fill the team-decay checklist (Signal 5).
3. **Watch the Dashboard** — GPs that fire enough signals appear ranked,
   with 🆕 badges until you open their detail page.
4. **Add a contact and log a call** — GP Detail → Contacts, then
   Timeline & Tasks → log an outbound call with outcome "No answer".
   The cadence engine instantly creates your follow-up task
   (+3 business days), which appears on the Today page.
5. **Kill a misfit** — Kill button on the Dashboard row or GP Detail.
   Pick a reason; the GP moves to the Graveyard permanently (resurrect
   anytime).
6. **Tune the model** — Signal Settings: toggles, thresholds, weights,
   saved presets, and your follow-up cadence.
7. **Export to Salesforce** — Export/Sync: Accounts, Contacts, and Tasks
   CSVs with stable external IDs (ZFS-…), plus an import path to sync
   statuses back from a Salesforce report.

## What's automated vs. manual

| Signal | How it works |
|---|---|
| 1 Form D vintage | **Automated** — Refresh EDGAR pulls each GP's Form D history; Discover surfaces unknown sponsors from old filings |
| 2 ADV decline | **Semi-auto** — drop in the SEC's monthly ADV bulk file; trends computed across snapshots |
| 3 Stale GP site | **Automated** — Wayback Machine content-change checks |
| 4 Long holds | Manual acquisition dates + lookup-assist links |
| 5 Team decay | Manual LinkedIn checklist (never scraped) |
| 6 Pension data | **Semi-auto** — upload CalPERS/other pension files; fuzzy-matched with your confirmation; ✅ verified badge |
| 7 No exits | Manual confirm with web-search helper |
| 8 Company decay | Wayback (auto) + manual checklist, AND/OR configurable |
| 9 Provider changes | **Semi-auto** — Schedule D 7.B.1 diffs across ADV snapshots + manual extension notes |
| 10 UCC liens | Guided manual workflow with state SoS links |

Fuzzy matches below the confidence bar are never auto-merged — they
wait in Data Manager → Confirm matches for your yes/no.

Your manual work — statuses, kills, notes, checklists, contacts,
activities, tasks — is stored separately from refresh-owned evidence
tables and survives every refresh and redeploy.

## Data safety

- Everything lives in `zombie_screener.db` (SQLite, created on first run).
- On Streamlit Cloud, the whole database mirrors to the `data` branch of
  your GitHub repo after every change and merges back on load, so
  redeploys never lose work. Kill decisions are specifically protected:
  a restored backup can re-kill but never resurrect a GP you killed.
