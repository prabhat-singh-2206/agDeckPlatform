import pandas as pd
import requests
import urllib.parse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

def get_area_governance_report(org, project, days, auth, story_types):
    # Schema initialization
    columns = ["Squad Name", "Total Stories", "Closed Stories", "Velocity (Points)", "Bugs Found", "Health Score", "Full Area Path"]
    since_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

    # API URLs
    proj_encoded = urllib.parse.quote(project)
    wiql_url = f"https://dev.azure.com/{org}/{proj_encoded}/_apis/wit/wiql?api-version=7.1"
    
    # 1. Fetch IDs
    query = {"query": f"SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = '{project}' AND [System.ChangedDate] >= '{since_date}'"}
    res = requests.post(wiql_url, json=query, auth=auth)
    
    if res.status_code != 200:
        return pd.DataFrame(columns=columns)
    
    ids = [wi['id'] for wi in res.json().get('workItems', [])]
    if not ids:
        return pd.DataFrame(columns=columns)

    # 2. Fetch Data
    batch_url = f"https://dev.azure.com/{org}/_apis/wit/workitemsbatch?api-version=7.1"
    fields = ["System.Id", "System.WorkItemType", "System.State", "System.AreaPath", "Microsoft.VSTS.Scheduling.StoryPoints"]
    wi_data = []
    
    for i in range(0, len(ids), 200):
        payload = {"ids": ids[i:i+200], "fields": fields}
        r = requests.post(batch_url, json=payload, auth=auth)
        if r.status_code == 200:
            wi_data.extend(r.json().get("value", []))

    # 3. Process
    stats = defaultdict(lambda: {"Stories": 0, "Bugs": 0, "Closed": 0, "Points": 0})
    for item in wi_data:
        f = item.get("fields", {})
        wtype = f.get("System.WorkItemType")
        area = f.get("System.AreaPath", "Unassigned")
        state = f.get("System.State")
        pts = f.get("Microsoft.VSTS.Scheduling.StoryPoints", 0) or 0

        if wtype in story_types:
            stats[area]["Stories"] += 1
            stats[area]["Points"] += pts
            if state in ["Closed", "Done", "Resolved", "Completed"]:
                stats[area]["Closed"] += 1
        elif wtype == "Bug":
            stats[area]["Bugs"] += 1

    # 4. Final Rows
    rows = []
    for path, d in stats.items():
        if d["Stories"] == 0 and d["Bugs"] == 0: continue
        rows.append({
            "Squad Name": path.split('\\')[-1],
            "Total Stories": d["Stories"],
            "Closed Stories": d["Closed"],
            "Velocity (Points)": d["Points"],
            "Bugs Found": d["Bugs"],
            "Health Score": round((d["Closed"]/d["Stories"]*100), 1) if d["Stories"] > 0 else 0,
            "Full Area Path": path
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns)