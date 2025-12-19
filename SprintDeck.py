import streamlit as st
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import re
import plotly.express as px

# ======================
# CONFIG & BEAUTIFICATION
# ======================
st.set_page_config(page_title="Sprint Execution Matrix", page_icon="🚀", layout="wide")

st.markdown("""
    <style>
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-top: 4px solid #0078d4; text-align: center; }
    .section-header { color: #0078d4; font-size: 24px; font-weight: 600; margin-top: 30px; border-bottom: 2px solid #e0e0e0; padding-bottom: 8px; text-align: center; }
    table { margin-left: auto; margin-right: auto; width: 100% !important; }
    th { background-color: #0078d4 !important; color: white !important; text-align: center !important; vertical-align: middle !important; }
    td { text-align: center !important; vertical-align: middle !important; }
    a { text-decoration: none !important; color: #0078d4 !important; font-weight: bold; }
    a:hover { text-decoration: underline !important; }
    </style>
""", unsafe_allow_html=True)

ORG = "lloydsregistergroup"
PAT = st.secrets["AZURE_DEVOPS_PAT"]
AUTH = HTTPBasicAuth("", PAT)
HEADERS = {"Content-Type": "application/json"}

STORY_TYPES = ["User Story", "Requirement", "Product Backlog Item"]
CLOSED_STATES = {"Closed", "Resolved", "Done", "Completed"}

# ======================
# API HELPERS
# ======================
@st.cache_data(ttl=3600)
def get_all_projects():
    try:
        r = requests.get(f"https://dev.azure.com/{ORG}/_apis/projects?api-version=6.0", auth=AUTH)
        return sorted([p['name'] for p in r.json().get('value', [])])
    except: return []

@st.cache_data(ttl=3600)
def get_iteration_paths(project_name):
    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project_name)}/_apis/wit/classificationNodes/Iterations?$depth=5&api-version=7.0"
    try:
        r = requests.get(url, auth=AUTH)
        all_paths = []
        def walk(node, current_path):
            name = node.get('name', '')
            new_path = f"{current_path}\\{name}" if current_path else name
            all_paths.append(new_path)
            for child in node.get('children', []): walk(child, new_path)
        walk(r.json(), "")
        return sorted(all_paths)
    except: return []

def get_pr_creator(url):
    try:
        parts = urllib.parse.unquote(url).split('/')
        api = f"https://dev.azure.com/{ORG}/_apis/git/repositories/{parts[-2]}/pullrequests/{parts[-1]}?api-version=7.0"
        res = requests.get(api, auth=AUTH).json()
        return res.get("createdBy", {}).get("displayName", "Unknown")
    except: return None

def fetch_details(ids):
    wi_map = {}
    if not ids: return wi_map
    ids = list(set([str(i) for i in ids]))
    for i in range(0, len(ids), 200):
        batch = ids[i:i+200]
        url = f"https://dev.azure.com/{ORG}/_apis/wit/workitems?ids={','.join(batch)}&$expand=relations&api-version=7.0"
        r = requests.get(url, auth=AUTH)
        if r.status_code == 200:
            for item in r.json().get("value", []):
                f = item.get("fields", {})
                wid = item["id"]
                rel_ids = []
                pr_links = []
                for rel in item.get('relations', []):
                    rel_url = rel.get('url', '')
                    m = re.search(r'/workItems/(\d+)$', rel_url)
                    if m: rel_ids.append(m.group(1))
                    if 'PullRequestId' in rel_url or 'pullRequests' in rel_url:
                        pr_links.append(rel_url)
                
                wi_map[item["id"]] = {
                "id": item["id"],
                "type": f.get("System.WorkItemType"),
                "state": f.get("System.State"),
                "title": f.get("System.Title"),
                "assigned_to": f.get("System.AssignedTo", {}).get("displayName", "Unassigned"),
                "created_by": f.get("System.CreatedBy", {}).get("displayName", "Unknown"),
                "story_points": f.get("Microsoft.VSTS.Scheduling.StoryPoints", 0),
                "pr_links": pr_links,
                "raw_links": rel_ids
            }
    return wi_map

def get_sprint_contributors(data_map, pr_lookup):
    contributors = defaultdict(lambda: {"Stories":0,"Bugs":0,"Test Cases":0,"PRs":0})
    for _, item in data_map.items():
        assigned = item.get("assigned_to")
        creator = item.get("created_by")
        wtype = item.get("type")
        if assigned and assigned != "Unassigned":
            if wtype in STORY_TYPES: contributors[assigned]["Stories"] += 1
            elif wtype == "Bug": contributors[assigned]["Bugs"] += 1
            elif wtype == "Test Case": contributors[assigned]["Test Cases"] += 1
        if wtype == "Bug" and creator: contributors[creator]["Bugs"] += 1
        for pr_url in item.get("pr_links", []):
            dev = pr_lookup.get(pr_url)
            if dev: contributors[dev]["PRs"] += 1
    return contributors

@st.cache_data(ttl=3600, show_spinner=False)
def get_developer_when_in_progress(work_item_id, project):
    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/workItems/{work_item_id}/revisions?api-version=7.0"
    response = requests.get(url, auth=AUTH)

    if response.status_code != 200:
        return "Unknown"

    for rev in response.json().get("value", []):
        fields = rev.get("fields", {})
        state = fields.get("System.State", "")
        assigned = fields.get("System.AssignedTo", {})

        if state in ["In Progress", "Active"]:
            if isinstance(assigned, dict):
                return assigned.get("displayName", "Unknown")
            return assigned

    return "Not Found"



# ======================
# MAIN UI
# ======================
st.title("📊 Sprint Execution Matrix")

with st.container(border=True):
    c1, c2, c3 = st.columns([3, 4, 2], vertical_alignment="bottom")
    with c1: sel_project = st.selectbox("📂 Project", get_all_projects())
    with c2: sel_iteration = st.selectbox("🏁 Iteration Path", get_iteration_paths(sel_project) if sel_project else [])
    with c3: load_btn = st.button("🚀 Load Dashboard", type="primary", use_container_width=True)

if load_btn and sel_iteration:
    with st.spinner("🔄 Fetching Data..."):
        query = f"SELECT [System.Id] FROM WorkItems WHERE [System.IterationPath] UNDER '{sel_iteration}'"
        api_url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(sel_project)}/_apis/wit/wiql?api-version=7.0"
        r = requests.post(api_url, json={"query": query}, auth=AUTH, headers=HEADERS)
        
        if r.status_code == 200:
            sprint_ids = [wi['id'] for wi in r.json().get('workItems', [])]
            data_map = fetch_details(sprint_ids)
            story_ids = [sid for sid, i in data_map.items() if i["type"] in STORY_TYPES]
            developer_worked_map = {}

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(
                    lambda sid: (sid, get_developer_when_in_progress(sid, sel_project)),
                    story_ids
                )

            for sid, dev in results:
                developer_worked_map[sid] = dev
            
            # --- Metrics calculation ---
            m_stats = {"ts": 0, "cs": 0, "bi": 0, "bf": 0, "tc": 0}
            qa_activity, bug_creators, linkage_table = defaultdict(int), defaultdict(list), []
            linked_bug_ids, all_pr_urls = set(), set()

            for sid, item in data_map.items():
                t, s, assigned, creator = item["type"], item["state"], item["assigned_to"], item["created_by"]
                link_html = f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>'
                for url in item["pr_links"]: all_pr_urls.add(url)

                if t in STORY_TYPES:
                    developer_worked = developer_worked_map.get(sid, "N/A")
                    m_stats["ts"] += 1
                    if s in CLOSED_STATES: m_stats["cs"] += 1
                    
                    bugs_links = []
                    for lid in item["raw_links"]:
                        b_item = data_map.get(int(lid))
                        if b_item and b_item["type"] == "Bug":
                            linked_bug_ids.add(int(lid))
                            b_link = f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{lid}" target="_blank">{lid}</a> ({b_item["state"]})'
                            bugs_links.append(b_link)
                    
                    linkage_table.append({
                        "Work Item Type": t,
                        "ID": link_html,
                        "Title": item["title"],
                        "Current Status": s,
                        "Story Points": item.get("story_points", 0),
                        "Linked Bugs (Clickable)": ", ".join(bugs_links) if bugs_links else "—",
                        "Developer Worked": developer_worked,   # ✅ NEW COLUMN
                        "Linked Bugs (Clickable)": ", ".join(bugs_links) if bugs_links else "—"
                    })

                elif t == "Bug":
                    m_stats["bi"] += 1
                    if s in CLOSED_STATES: m_stats["bf"] += 1
                    bug_creators[creator].append(f"{link_html} ({s})")
                elif t == "Test Case":
                    m_stats["tc"] += 1
                    qa_activity[assigned] += 1

            # Independent Bugs
            for sid, item in data_map.items():
                if item["type"] == "Bug" and sid not in linked_bug_ids:
                    link_html = f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>'
                    linkage_table.append({
                        "Work Item Type": "Independent Bug",
                        "ID": link_html,
                        "Title": item["title"],
                        "Current Status": item["state"],
                        "Story Points": item.get("story_points", 0),
                        "Linked Bugs (Clickable)": "N/A"
                    })

            # --- Sprint KPI Performance ---
            st.markdown('<div class="section-header">📈 Sprint KPI Performance</div>', unsafe_allow_html=True)

            story_perc = int((m_stats["cs"]/m_stats["ts"])*100) if m_stats["ts"] > 0 else 0
            bug_perc = int((m_stats["bf"]/m_stats["bi"])*100) if m_stats["bi"] > 0 else 0
            active_dev_qa = len(set([item["assigned_to"] for item in data_map.values() if item["assigned_to"] != "Unassigned"]))

            def get_health_color(perc):
                if perc >= 80:
                    return "🟢 Healthy"
                elif perc >= 50:
                    return "🟡 Moderate"
                else:
                    return "🔴 Critical"

            health_status = get_health_color(story_perc)

            # Metrics cards
            k1, k2, k3, k4, k5 = st.columns([1,1,1,1,1])
            with k1:
                st.metric(label="📄 Total Stories", value=m_stats["ts"])
            with k2:
                st.metric(label="✅ Stories Closed", value=m_stats["cs"], delta=f"{story_perc}%")
            with k3:
                st.metric(label="🐞 Bugs Identified", value=m_stats["bi"])
            with k4:
                st.metric(label="🔧 Bugs Fixed", value=m_stats["bf"], delta=f"{bug_perc}%")
            with k5:
                st.metric(label="🧪 Test Cases Written", value=m_stats["tc"])

            k6, k7 = st.columns([2,1])
            with k6:
                st.metric(label="👨‍💻 Active Developers/QA", value=active_dev_qa)
            with k7:
                st.metric(label="💖 Sprint Health", value=health_status)

            # --- Pie chart for work item distribution ---
            pie_data = pd.DataFrame({
                "Work Item Type": ["Stories", "Bugs", "Test Cases"],
                "Count": [m_stats["ts"], m_stats["bi"], m_stats["tc"]]
            })
            st.markdown('<div class="section-header">📊 Work Item Distribution</div>', unsafe_allow_html=True)
            st.plotly_chart(
                px.pie(pie_data, names="Work Item Type", values="Count", color="Work Item Type",
                       color_discrete_map={"Stories":"#0078d4","Bugs":"#d62728","Test Cases":"#ff7f0e"},
                       hole=0.3).update_traces(textinfo='percent+label'), use_container_width=True
            )

            # --- User Story & Bug Linkage Matrix ---
            st.markdown('<div class="section-header">🔗 User Story & Bug Linkage Matrix</div>', unsafe_allow_html=True)
            st.write(pd.DataFrame(linkage_table).to_html(escape=False, index=False), unsafe_allow_html=True)

            # --- Developer PR Activity ---
            st.markdown('<div class="section-header">👨‍💻 Developers Activity</div>', unsafe_allow_html=True)
            pr_lookup = {}  # ✅ ALWAYS initialize
            dev_map = defaultdict(set)
            if all_pr_urls:
                with ThreadPoolExecutor(max_workers=10) as exe:
                    results = list(exe.map(get_pr_creator, list(all_pr_urls)))
                pr_lookup = dict(zip(all_pr_urls, results))

                for sid, item in data_map.items():
                    link_html = f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>'
                    for url in item["pr_links"]:
                        name = pr_lookup.get(url)
                        if name:
                            dev_map[name].add(f"{link_html} ({item['state']})")
            if dev_map:
                df_dev = pd.DataFrame([{"Developer": d, "Items": len(i), "Work Items": ", ".join(list(i))} for d, i in dev_map.items()])
                st.write(df_dev.to_html(escape=False, index=False), unsafe_allow_html=True)

            # --- Sprint Contributors ---
            st.markdown('<div class="section-header">👥 Sprint Contributors</div>',unsafe_allow_html=True)
            contributors = get_sprint_contributors(data_map, pr_lookup)
            if contributors:
                df_contrib = pd.DataFrame([
                    {
                        "Contributor": name,
                        "Stories Worked": v["Stories"],
                        "Bugs Worked": v["Bugs"],
                        "Test Cases": v["Test Cases"],
                        "Pull Requests": v["PRs"]
                    }
                    for name, v in contributors.items()
                ])
                st.write(df_contrib.to_html(index=False), unsafe_allow_html=True)
            else:
                st.info("No contributors found for this sprint.")

            # --- QA Activity & Bugs Logged ---
            st.markdown('<div class="section-header">👩‍🔬 QA Activity & Bugs Logged</div>', unsafe_allow_html=True)
            q1, q2 = st.columns(2)
            with q1:
                st.write("**Test Cases Created**")
                st.write(pd.DataFrame([{"QA Name": n, "Count": c} for n, c in qa_activity.items()]).to_html(index=False), unsafe_allow_html=True)
            with q2:
                st.write("**Bugs Created By**")
                # --- FIXED BUG TABLE WITH TOTAL BUGS & CENTER ALIGN ---
                st.markdown("""
                    <style>
                    .stMetric {
                        background-color: #ffffff;
                        padding: 20px;
                        border-radius: 12px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                        border-top: 4px solid #0078d4;
                        text-align: left;   /* ✅ LEFT */
                    }

                    .section-header {
                        color: #0078d4;
                        font-size: 24px;
                        font-weight: 600;
                        margin-top: 30px;
                        border-bottom: 2px solid #e0e0e0;
                        padding-bottom: 8px;
                        text-align: left;   /* ✅ LEFT */
                    }

                    table {
                        margin-left: auto;
                        margin-right: auto;
                        width: 100% !important;
                        border-collapse: collapse;
                    }

                    th {
                        background-color: #0078d4 !important;
                        color: white !important;
                        text-align: left !important;      /* ✅ LEFT */
                        vertical-align: middle !important;
                        padding: 8px;
                    }

                    td {
                        text-align: left !important;      /* ✅ LEFT */
                        vertical-align: middle !important;
                        padding: 6px;
                    }

                    a {
                        text-decoration: none !important;
                        color: #0078d4 !important;
                        font-weight: bold;
                    }

                    a:hover {
                        text-decoration: underline !important;
                    }
                    </style>
                """, unsafe_allow_html=True)


                bug_data = []
                for creator, bug_list in bug_creators.items():
                    bug_data.append({
                        "Creator": creator,
                        "Bugs": ", ".join(bug_list),
                        "Total Bugs": len(bug_list)
                    })

                st.write(f'<div class="bug-table">{pd.DataFrame(bug_data).to_html(escape=False, index=False)}</div>', unsafe_allow_html=True)
