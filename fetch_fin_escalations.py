#!/usr/bin/env python3
"""
fetch_fin_escalations_daily.py
Exports **yesterday-only** data and writes two CSVs:

  • fin_escalations_YYYY-MM-DD.csv   – id,created_at,rating,user_messages
  • fin_conversations_YYYY-MM-DD.csv – id,created_at,rating,full_conversation
"""

import requests, csv, datetime as dt, pathlib, re, html, copy, zoneinfo

# ───────── 1. ACCESS TOKEN ───────────────────────────────────────────────
TOKEN = "INTERCOM_TOKEN"        # ← Intercom token

# ───────── 2. DATE WINDOW (Stockholm) ────────────────────────────────────
tz       = zoneinfo.ZoneInfo("Europe/Stockholm")
today    = dt.datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
y_start  = int((today - dt.timedelta(days=1)).timestamp())   # 00:00 yesterday (UTC-seconds)
y_end    = int(today.timestamp())                            # 00:00 today      (UTC-seconds)
y_date   = (today - dt.timedelta(days=1)).date()             # YYYY-MM-DD string

# ───────── 3. API CONFIG ────────────────────────────────────────────────
API_ROOT = "https://api.intercom.io/conversations"
HEADERS  = {
    "Authorization": f"Bearer {TOKEN}",
    "Intercom-Version": "2.13",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ───────── 4. SHARED TIME FILTER CLAUSES ────────────────────────────────
TIME_FILTER = [
    {"field": "updated_at", "operator": ">", "value": y_start},
    {"field": "updated_at", "operator": "<", "value": y_end},
]

# ───────── 5. SEARCH DEFINITIONS ────────────────────────────────────────
ESCALATED = {
    "query": {
        "operator": "AND",
        "value": [
            {"field":"ai_agent_participated","operator":"=","value":True},
            {"field":"ai_agent.resolution_state","operator":"=","value":"routed_to_team"},
            *TIME_FILTER,
        ],
    },
    "pagination":{"per_page":100},
}

LOW_CSAT = {
    "query": {
        "operator":"AND",
        "value":[
            {"field":"ai_agent_participated","operator":"=","value":True},
            {"field":"ai_agent.rating","operator":"<","value":2},
            *TIME_FILTER,
        ],
    },
    "pagination":{"per_page":100},
}

# ───────── 6. UTILITIES ─────────────────────────────────────────────────
TAG_RE = re.compile(r"<[^>]+>")
def strip_html(t:str)->str:
    return html.unescape(TAG_RE.sub("",t or "")).strip()

def fetch_full(cid:str)->dict:
    r=requests.get(f"{API_ROOT}/{cid}?display_as=plaintext",
                   headers=HEADERS,timeout=30); r.raise_for_status(); return r.json()

def first_assignment(parts)->int|None:
    for p in parts:
        if p.get("part_type")=="assignment" and "created_at" in p:
            return p["created_at"]
    return None

def split(conv:dict)->tuple[str,str]:
    """Return (user_only , full_dialogue) up to first hand-off."""
    fin_id = (conv.get("ai_agent", {}).get("actor", {}) or
              conv.get("ai_agent", {})).get("id")

    parts  = []
    root   = conv.get("conversation_message") or conv.get("source")
    if root: parts.append(root)
    parts += conv.get("conversation_parts", {}).get("conversation_parts", [])

    cutoff = first_assignment(parts)
    user_lines, convo_lines = [], []

    for p in parts:
        ts = p.get("created_at")
        if cutoff and ts and ts >= cutoff: break
        body = strip_html(p.get("body","")) or None
        if not body: continue

        a = p.get("author", {})
        is_fin = (
            p.get("part_type")=="ai_answer" or
            "ai_answer_type" in p or
            a.get("type")=="bot" or
            (fin_id and a.get("id")==fin_id)
        )
        if is_fin:
            convo_lines.append(f"[Fin]  {body}")
        elif a.get("type") in {"contact","user","lead"}:
            user_lines.append(body); convo_lines.append(f"[User] {body}")

    return ("\n\n".join(user_lines), "\n\n".join(convo_lines))

def run_search(template):
    body=copy.deepcopy(template)
    while True:
        r=requests.post(f"{API_ROOT}/search",json=body,headers=HEADERS,timeout=30)
        r.raise_for_status(); pg=r.json()
        yield from pg.get("conversations",[])
        nxt=pg.get("pages",{}).get("next",{}).get("starting_after")
        if not nxt: break
        body["pagination"]["starting_after"]=nxt

# ───────── 7. COLLECT & DEDUP ───────────────────────────────────────────
seen=set(); user_rows=[]; convo_rows=[]

def ingest(s):
    cid=s["id"]
    if cid in seen: return
    seen.add(cid)
    conv=fetch_full(cid)
    user_txt,full_txt=split(conv)
    iso=dt.datetime.utcfromtimestamp(s["created_at"]).isoformat()
    rating=s["ai_agent"].get("rating")
    user_rows.append({"id":cid,"created_at":iso,"rating":rating,
                      "user_messages":user_txt})
    convo_rows.append({"id":cid,"created_at":iso,"rating":rating,
                       "full_conversation":full_txt})

for c in run_search(ESCALATED): ingest(c)
for c in run_search(LOW_CSAT):  ingest(c)

# ───────── 8. WRITE CSVs ────────────────────────────────────────────────
base = pathlib.Path(__file__).parent
esc_f = base / f"fin_escalations_{y_date}.csv"
con_f = base / f"fin_conversations_{y_date}.csv"

with esc_f.open("w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=["id","created_at","rating","user_messages"])
    w.writeheader(); w.writerows(user_rows)

with con_f.open("w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=["id","created_at","rating","full_conversation"])
    w.writeheader(); w.writerows(convo_rows)

print(f"✅  {len(user_rows)} conversations saved to {esc_f.name} & {con_f.name}")
