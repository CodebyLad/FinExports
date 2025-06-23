#!/usr/bin/env python3
import os, pathlib, datetime as dt, zoneinfo, requests

# ---------- locate yesterday's files ----------
tz       = zoneinfo.ZoneInfo("Europe/Stockholm")
y_date   = (dt.datetime.now(tz) - dt.timedelta(days=1)).date()
base     = pathlib.Path(__file__).parent
files    = [base / f"fin_escalations_{y_date}.csv",
            base / f"fin_conversations_{y_date}.csv"]

# ---------- Slack details ----------
TOKEN      = os.getenv("SLACK_BOT_TOKEN")      # will be injected by GitHub
CHANNEL_ID = "C01234567"                       # ← paste your channel ID
TAG_LINE   = "<@U089ABCD>"                     # ← optional mentions; or ""

# ---------- upload ----------
for f in files:
    comment = (f"📊 *Fin AI report – {y_date}*\n{TAG_LINE}"
               if "escalations" in f.name else "")
    r = requests.post(
        "https://slack.com/api/files.upload",
        headers={"Authorization": f"Bearer {TOKEN}"},
        data={"channels": CHANNEL_ID,
              "initial_comment": comment},
        files={"file": (f.name, open(f, "rb"), "text/csv")},
        timeout=30)
    r.raise_for_status()
print("✅  CSVs delivered to Slack")
