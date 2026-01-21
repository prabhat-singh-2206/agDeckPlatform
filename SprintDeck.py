import streamlit as st
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import re
import plotly.express as px
from datetime import datetime
from io import BytesIO
from resource_view import render_resource_view
from governance_service import get_area_governance_report
import plotly.express as px
import io
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# ======================
# CONFIG & BEAUTIFICATION
# ======================
st.set_page_config(page_title="Sprint Execution Matrix", page_icon="🚀", layout="wide")

st.markdown("""
    <style>
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-top: 4px solid #0078d4; text-align: center; }
    .resource-card { background-color: #f8f9fa; padding: 20px; border-radius: 15px; border: 1px solid #e0e0e0; margin-bottom: 20px; }
    .resource-name { color: #0078d4; font-size: 18px; font-weight: bold; margin-bottom: 10px; border-bottom: 1px solid #ddd; }
    .section-header { color: #0078d4; font-size: 24px; font-weight: 600; margin-top: 30px; border-bottom: 2px solid #e0e0e0; padding-bottom: 8px; text-align: left; }
    .health-card { padding: 20px; border-radius: 12px; text-align: center; color: white; font-weight: bold; font-size: 20px; }
    table { width: 100% !important; border-collapse: collapse; margin-top: 10px; }
    th { background-color: #0078d4 !important; color: white !important; text-align: left !important; padding: 8px; }
    td { text-align: left !important; padding: 6px; border-bottom: 1px solid #eee; }
    a { text-decoration: none !important; color: #0078d4 !important; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

ORG = "lloydsregistergroup"
PAT = st.secrets["AZURE_DEVOPS_PAT"]
#PAT = ""  # Replace with your actual PAT or use secrets management
AUTH = HTTPBasicAuth("", PAT)
HEADERS = {"Content-Type": "application/json"}
STORY_TYPES = ["User Story", "Requirement", "Product Backlog Item"]
CLOSED_STATES = {"Closed", "Resolved", "Done", "Completed"}
date_map_lookup = {}

# ======================
# API HELPERS
# ======================
@st.cache_data(ttl=3600)
def get_all_projects(org, _auth):  # Added underscore to _auth
    url = f"https://dev.azure.com/{org}/_apis/projects?api-version=7.1&$top=1000"
    try:
        res = requests.get(url, auth=_auth) # Use the underscored name inside
        if res.status_code == 200:
            projects = [p['name'] for p in res.json()['value']]
            return sorted(projects)
    except Exception as e:
        st.error(f"Error loading projects: {e}")
    return []

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

@st.cache_data(ttl=3600)
def get_area_paths(project_name):
    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project_name)}/_apis/wit/classificationNodes/Areas?$depth=5&api-version=7.0"
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
                rel_ids, pr_links = [], []
                for rel in item.get('relations', []):
                    rel_url = rel.get('url', '')
                    m = re.search(r'/workItems/(\d+)$', rel_url)
                    if m: rel_ids.append(m.group(1))
                    if 'PullRequestId' in rel_url or 'pullRequests' in rel_url: pr_links.append(rel_url)
                
                wi_map[item["id"]] = {
                    "id": item["id"],
                    "type": f.get("System.WorkItemType"),
                    "state": f.get("System.State"),
                    "title": f.get("System.Title"),
                    "assigned_to": f.get("System.AssignedTo", {}).get("displayName", "Unassigned") if isinstance(f.get("System.AssignedTo"), dict) else "Unassigned",
                    "created_by": f.get("System.CreatedBy", {}).get("displayName", "Unknown") if isinstance(f.get("System.CreatedBy"), dict) else "Unknown",
                    "story_points": f.get("Microsoft.VSTS.Scheduling.StoryPoints", 0),
                    "pr_links": pr_links, "raw_links": rel_ids
                }
    return wi_map

@st.cache_data(ttl=3600, show_spinner=False)
def get_developer_when_in_progress(work_item_id, project):
    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/workItems/{work_item_id}/revisions?api-version=7.0"
    try:
        response = requests.get(url, auth=AUTH)
        if response.status_code == 200:
            for rev in response.json().get("value", []):
                fields = rev.get("fields", {})
                if fields.get("System.State", "") in ["In Progress", "Active"]:
                    assigned = fields.get("System.AssignedTo", {})
                    return assigned.get("displayName", "Unknown") if isinstance(assigned, dict) else assigned
    except: pass
    return "Not Found"

def reset_search():
    st.session_state.search_attempted = False
    st.session_state.gov_results = None

# ======================
# MAIN UI
# ======================
st.image("aventra_logo.png", width=120)
st.title("📊 Delivery Execution Matrix")

# ======================
# VIEW SELECTION
# ======================

# 1. Update the View Selection
view_mode = st.radio(
    "Select View",
    ["Delivery Execution", "Resource Execution", "Squad Governance"],
    horizontal=True
)

if view_mode == "Squad Governance":
    st.title("🛡️ Project Governance & Squad Health")
    
    # 1. Initialize session state
    if 'gov_results' not in st.session_state:
        st.session_state.gov_results = None
    if 'search_attempted' not in st.session_state:
        st.session_state.search_attempted = False

    all_projects = get_all_projects(ORG, AUTH)
    
    with st.expander("⚙️ Report Settings", expanded=True):
        col_a, col_b = st.columns(2)
        with col_a: 
            sel_proj = st.selectbox("Select Project", all_projects, on_change=reset_search)
        with col_b: 
            lookback = st.number_input("Lookback Window (Days)", value=30, min_value=1)
        
        run_btn = st.button("🚀 Run Analysis", type="primary")

    # 2. DATA PROCESSING
    if run_btn:
        st.session_state.search_attempted = True
        with st.spinner("Crunching Azure DevOps Data..."):
            df_result = get_area_governance_report(ORG, sel_proj, lookback, AUTH, STORY_TYPES)
            
            if not df_result.empty and (df_result["Total Stories"].sum() + df_result["Bugs Found"].sum() > 0):
                st.session_state.gov_results = {
                    "df": df_result,
                    "project": sel_proj,
                    "timestamp": datetime.now().strftime("%Y%m%d_%H%M")
                }
            else:
                st.session_state.gov_results = None

    # 3. UI RENDERING
    # 3. UI RENDERING
    if st.session_state.gov_results:
        res = st.session_state.gov_results
        df = res["df"]
        # --- SAFETY FIX: ensure bug columns always exist ---
        for col in ["SIT Bugs", "UAT Bugs"]:
            if col not in df.columns:
                df[col] = 0

        # --- Health Summary Row ---
        h1, h2 = st.columns(2)
        h1.metric("Active Squads", len(df))
        h2.metric("Avg Health Score", f"{df['Health Score'].mean():.1f}%")
        
        # --- Summary Tile Section (Expanded to 6 columns) ---
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        
        # 1. Total Stories
        m1.metric("Planned Stories", int(df["Total Stories"].sum()))
        
        # 2. Total Closed Stories (New Requirement)
        m2.metric("Planned Closed Stories", int(df["Closed Stories"].sum()))
        
        # 3. Total Velocity (New Requirement)
        m3.metric("Total Velocity", int(df["Velocity (Points)"].sum()))
        
        # 4. Total Bugs Found (New Requirement)
        m4.metric("Total Bugs", int(df["Bugs Found"].sum()))
        
        # 5. QA/UAT Split
        # Using the inclusive logic where QA = Total - UAT if tags are missing
        m5.metric("SIT Bugs", int(df["SIT Bugs"].sum()))
        m6.metric("UAT Bugs", int(df["UAT Bugs"].sum()))

        st.markdown("---") # Visual separator

        

        # --- Plotly Chart (Unchanged) ---
        fig_health = px.bar(
            df, x="Squad Name", y="Health Score", 
            color="Health Score", color_continuous_scale="RdYlGn",
            title=f"Squad Health Overview: {res['project']}"
        )
        st.plotly_chart(fig_health, use_container_width=True, key="gov_plotly")
        
        # --- Data Table (Now shows QA Bugs and UAT Bugs automatically from df) ---
        st.dataframe(
            df.drop(columns=["Full Area Path"], errors='ignore'), 
            hide_index=True, 
            use_container_width=True
        )

        # --- 4. MATPLOTLIB FOR EXCEL (Unchanged) ---
        image_data = io.BytesIO()
        try:
            plt.figure(figsize=(10, 5))
            norm = mcolors.Normalize(vmin=0, vmax=100) 
            cmap = plt.get_cmap('RdYlGn')
            bar_colors = [cmap(norm(value)) for value in df["Health Score"]]
            bars = plt.bar(df["Squad Name"], df["Health Score"], color=bar_colors, edgecolor='black', linewidth=0.5)
            
            for bar in bars:
                height = bar.get_height()
                plt.text(
                    bar.get_x() + bar.get_width()/2.,
                    height + 1,
                    f'{height:.1f}%',
                    ha='center', va='bottom',
                    fontsize=10, fontweight='bold'
                )

            plt.title(f"Squad Health Overview: {res['project']}", fontsize=14, pad=20)
            plt.ylabel("Health Score (%)", fontsize=12)
            plt.ylim(0, 110)
            plt.xticks(rotation=45, ha='right')
            plt.grid(axis='y', linestyle='--', alpha=0.3)
            plt.tight_layout()
            
            plt.savefig(image_data, format='png', dpi=100)
            plt.close()
            image_data.seek(0)
        except Exception as e:
            image_data = None
            st.error(f"Excel Chart Error: {e}")

        # --- 5. EXCEL EXPORT ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # The exported excel sheet will also contain the new QA/UAT columns
            df.to_excel(writer, index=False, sheet_name='Data_Report')
            
            if image_data:
                workbook = writer.book
                worksheet = workbook.add_worksheet('Dashboard')
                header_format = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#0078d4'})
                worksheet.write('B2', f"Governance Report: {res['project']}", header_format)
                worksheet.write('B3', f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                worksheet.insert_image('B5', 'health_chart.png', {'image_data': image_data})

        # --- 6. DOWNLOAD BUTTON ---
        st.sidebar.download_button(
            label="📥 Download Excel Report",
            data=output.getvalue(),
            file_name=f"Gov_Report_{res['project']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="gov_export_stable"
        )
    
    elif st.session_state.search_attempted and st.session_state.gov_results is None:
        st.warning(f"No activity found for '{sel_proj}' in the last {lookback} days.")    

# --- RESOURCE EXECUTION VIEW ---
if view_mode == "Resource Execution":
    render_resource_view(AUTH)
    st.stop()   # ⛔ VERY IMPORTANT: stops Delivery/Kanban code


# --- DELIVERY EXECUTION VIEW ---
if view_mode == "Delivery Execution":
    with st.sidebar:
        st.header("⚙️ View Settings")
        is_kanban = st.toggle("🚀 Sprint ↔ Kanban")
        lookback_days = 30
        if is_kanban:
            time_choice = st.selectbox("⏳ Timeframe", ["Last 30 Days", "Last 60 Days","Last 90 Days", "Last 180 Days", "Last Year"])
            days_lookup = {"Last 30 Days": 30, "Last 60 Days": 60, "Last 90 Days": 90, "Last 180 Days": 180, "Last Year": 365}
            lookback_days = days_lookup[time_choice]

    with st.container(border=True):
        c1, c2, c3 = st.columns([3, 4, 2], vertical_alignment="bottom")
        with c1: sel_project = st.selectbox("📂 Project", get_all_projects(ORG, AUTH))
        with c2: 
            if is_kanban:
                sel_path = st.selectbox("📐 Area Path", get_area_paths(sel_project) if sel_project else [])
                path_filter = f"[System.AreaPath] UNDER '{sel_path}' AND [System.ChangedDate] >= @today - {lookback_days}"
            else:
                # UPDATED: Use the utility function that returns dates
                from iteration_utils import get_iteration_paths_with_dates
                
                paths, date_map_lookup = get_iteration_paths_with_dates(sel_project)
                sel_path = st.selectbox("🏁 Iteration Path", paths if sel_project else [])
                path_filter = f"[System.IterationPath] UNDER '{sel_path}'"

                # NEW: Display dates if they exist for the selected path
                if sel_path in date_map_lookup:
                    s_date = date_map_lookup[sel_path]['start']
                    e_date = date_map_lookup[sel_path]['end']
                    st.caption(f"📅 **Sprint Duration:** {s_date} to {e_date}")
        with c3: load_btn = st.button("🚀 Load Dashboard", type="primary", use_container_width=True)

    if load_btn and sel_path:
        with st.spinner("🔄 Fetching Data..."):
            query = f"SELECT [System.Id] FROM WorkItems WHERE {path_filter}"
            api_url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(sel_project)}/_apis/wit/wiql?api-version=7.0"
            r = requests.post(api_url, json={"query": query}, auth=AUTH, headers=HEADERS)
            
            if r.status_code == 200:
                sprint_ids = [wi['id'] for wi in r.json().get('workItems', [])]
                data_map = fetch_details(sprint_ids)
                story_ids = [sid for sid, i in data_map.items() if i["type"] in STORY_TYPES]
                
                with ThreadPoolExecutor(max_workers=10) as executor:
                    dev_results = dict(executor.map(lambda sid: (sid, get_developer_when_in_progress(sid, sel_project)), story_ids))
                
                m_stats = {"ts": 0, "cs": 0, "bi": 0, "bf": 0, "tc": 0}
                qa_activity, bug_creators, linkage_table = defaultdict(int), defaultdict(list), []
                active_users, all_pr_urls = set(), set()

                for sid, item in data_map.items():
                    t, s, assigned, creator = item["type"], item["state"], item["assigned_to"], item["created_by"]
                    if assigned != "Unassigned": active_users.add(assigned)
                    for url in item["pr_links"]: all_pr_urls.add(url)

                    if t in STORY_TYPES:
                        m_stats["ts"] += 1
                        if s in CLOSED_STATES: m_stats["cs"] += 1

                        # Linked bugs for this story
                        bugs_links = [
                            f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{int(lid)}" target="_blank">{lid}</a> ({data_map[int(lid)]["state"]})'
                            for lid in item["raw_links"]
                            if int(lid) in data_map and data_map[int(lid)]["type"] == "Bug"
                        ]

                        linkage_table.append({
                            "Type": t,
                            "ID": f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>',
                            "Title": item["title"],
                            "Status": s,
                            "Points": item.get("story_points", 0),
                            "Bugs": ", ".join(bugs_links) if bugs_links else "—",
                            "Dev": dev_results.get(sid, "N/A")
                        })

                    elif t == "Bug":
                        m_stats["bi"] += 1
                        if s in CLOSED_STATES: m_stats["bf"] += 1
                        bug_creators[creator].append(f'{sid} ({s})')

                        # Independent bug entry in linkage table
                        # Only if it is not linked to a story
                        is_linked = any(
                            sid in item2.get("raw_links", [])
                            for item2 in data_map.values()
                            if item2["type"] in STORY_TYPES
                        )
                        if not is_linked:
                            linkage_table.append({
                                "Type": t,
                                "ID": f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>',
                                "Title": item["title"],
                                "Status": s,
                                "Points": 0,
                                "Bugs": "—",
                                "Dev": dev_results.get(sid, "N/A")
                            })

                    elif t == "Test Case":
                        m_stats["tc"] += 1
                        qa_activity[assigned] += 1


                pr_lookup = {}
                if all_pr_urls:
                    with ThreadPoolExecutor(max_workers=10) as exe:
                        pr_lookup = dict(zip(all_pr_urls, list(exe.map(get_pr_creator, list(all_pr_urls)))))

                # --- KPI & HEALTH SECTION ---
                st.markdown('<div class="section-header">📈 KPI Performance Metrics</div>', unsafe_allow_html=True)
                # NEW: Show Dates at the top of KPIs
                # NEW: Display Sprint Dates in balanced, compact columns
                if not is_kanban and sel_path in date_map_lookup:
                    # Use small equal ratios and a large spacer to prevent stretching
                    d1, d2, spacer = st.columns([1.5, 1.5, 5]) 
                    
                    with d1:
                        st.info(f"🗓️ **Start:** {date_map_lookup[sel_path]['start']}")
                    with d2:
                        st.info(f"🏁 **End:** {date_map_lookup[sel_path]['end']}")

                s_perc = int((m_stats["cs"]/m_stats["ts"])*100) if m_stats["ts"] > 0 else 0
                s_perc = int((m_stats["cs"]/m_stats["ts"])*100) if m_stats["ts"] > 0 else 0
                b_perc = int((m_stats["bf"]/m_stats["bi"])*100) if m_stats["bi"] > 0 else 0
                
                health_label, health_color = ("🟢 Healthy", "#28a745") if s_perc > 70 else (("🟡 Warning", "#ffc107") if s_perc > 40 else ("🔴 Critical", "#dc3545"))

                k1, k2, k3, k4, k5, k6, k7 = st.columns([1,1,1,1,1,1,1.5])
                k1.metric("📄 Total Stories", m_stats["ts"])
                k2.metric("✅ Stories Closed", m_stats["cs"], f"{s_perc}%")
                k3.metric("🐞 Bugs Identified", m_stats["bi"])
                k4.metric("🔧 Bugs Fixed", m_stats["bf"], f"{b_perc}%")
                k5.metric("🧪 Test Cases", m_stats["tc"])
                k6.metric("👨‍💻 Active Team", len(active_users))
                with k7:
                    st.markdown(f'<div class="health-card" style="background-color: {health_color};">💖 Sprint Health: {health_label}</div>', unsafe_allow_html=True)

                # --- RESOURCE MATRIX (KANBAN ONLY) ---
                # --- RESOURCE PERFORMANCE MATRIX (KANBAN ONLY) ---
                # --- RESOURCE PERFORMANCE MATRIX (KANBAN ONLY | FIXED) ---
                if is_kanban:
                    st.markdown('<div class="section-header">👥 Team Contribution Matrix (Kanban)</div>', unsafe_allow_html=True)

                    # contribution[area][person] → counts
                    contribution = defaultdict(lambda: defaultdict(lambda: {
                        "User Stories": 0,
                        "Bugs": 0,
                        "PRs": 0
                    }))

                    # cache revision calls
                    @st.cache_data(ttl=3600, show_spinner=False)
                    def get_contributors(work_item_id, project):
                        url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/workItems/{work_item_id}/revisions?api-version=7.0"
                        users = set()
                        try:
                            r = requests.get(url, auth=AUTH)
                            if r.status_code == 200:
                                for rev in r.json().get("value", []):
                                    fields = rev.get("fields", {})
                                    changed_by = fields.get("System.ChangedBy")
                                    if isinstance(changed_by, dict):
                                        users.add(changed_by.get("displayName"))
                        except:
                            pass
                        return users

                    # --- Collect contributors ---
                    with ThreadPoolExecutor(max_workers=10) as exe:
                        revision_results = dict(
                            exe.map(lambda sid: (sid, get_contributors(sid, sel_project)), data_map.keys())
                        )

                    for wid, item in data_map.items():
                        area = item.get("area_path", sel_path)
                        wtype = item["type"]

                        # STORY / BUG contributions
                        if wtype in STORY_TYPES or wtype == "Bug":
                            for user in revision_results.get(wid, []):
                                if not user:
                                    continue
                                if wtype in STORY_TYPES:
                                    contribution[area][user]["User Stories"] += 1
                                elif wtype == "Bug":
                                    contribution[area][user]["Bugs"] += 1

                        # PR contributions
                        for pr_url in item["pr_links"]:
                            pr_owner = pr_lookup.get(pr_url)
                            if pr_owner:
                                contribution[area][pr_owner]["PRs"] += 1

                    # --- Render UI ---
                    for area, members in contribution.items():
                        st.subheader(f"📐 Area Path: {area}")

                        rows = []
                        for person, stats in members.items():
                            rows.append({
                                "Team Member": person,
                                **stats,
                                "Total": sum(stats.values())
                            })

                        df = pd.DataFrame(rows).sort_values("Total", ascending=False)

                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True
                        )

                    # --- Save for Excel ---
                    res_stats = {
                        f"{area} | {person}": stats
                        for area, members in contribution.items()
                        for person, stats in members.items()
                    }

                

                # --- DISTRIBUTION ---
                st.markdown('<div class="section-header">📊 Work Item Distribution</div>', unsafe_allow_html=True)
                st.plotly_chart(px.pie(pd.DataFrame({"Type": ["Stories", "Bugs", "Test Cases"], "Count": [m_stats["ts"], m_stats["bi"], m_stats["tc"]]}), names="Type", values="Count", hole=0.3), use_container_width=True)

                # --- LINKAGE MATRIX ---
                st.markdown('<div class="section-header">🔗 User Story & Bug Linkage Matrix</div>', unsafe_allow_html=True)
                st.write(pd.DataFrame(linkage_table).to_html(escape=False, index=False), unsafe_allow_html=True)

                # --- DEVELOPER PR ACTIVITY ---
                st.markdown('<div class="section-header">👨‍💻 Developers Activity (PRs)</div>', unsafe_allow_html=True)
                dev_pr_map = defaultdict(set)
                for sid, item in data_map.items():
                    link = f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{sid}" target="_blank">{sid}</a>'
                    for url in item["pr_links"]:
                        name = pr_lookup.get(url)
                        if name: dev_pr_map[name].add(f"{link} ({item['state']})")
                if dev_pr_map:
                    st.write(pd.DataFrame([{"Developer": d, "Items": len(v), "Work Items": ", ".join(list(v))} for d, v in dev_pr_map.items()]).to_html(escape=False, index=False), unsafe_allow_html=True)

                # --- SPRINT CONTRIBUTORS ---
                st.markdown('<div class="section-header">👥 Sprint Contributors</div>', unsafe_allow_html=True)
                contrib_data = defaultdict(lambda: {"Stories":0, "Bugs":0, "Test Cases":0, "PRs":0})
                for sid, item in data_map.items():
                    u, t = item["assigned_to"], item["type"]
                    if u != "Unassigned":
                        if t in STORY_TYPES: contrib_data[u]["Stories"] += 1
                        elif t == "Bug": contrib_data[u]["Bugs"] += 1
                        elif t == "Test Case": contrib_data[u]["Test Cases"] += 1
                    for url in item["pr_links"]:
                        p_name = pr_lookup.get(url)
                        if p_name: contrib_data[p_name]["PRs"] += 1
                if contrib_data:
                    st.write(pd.DataFrame([{"Contributor": k, **v} for k, v in contrib_data.items()]).to_html(index=False), unsafe_allow_html=True)
                
                st.markdown("""
                    <style>
                    /* Force tables to wrap text and not crop content */
                    table { width: 100% !important; }
                    td { 
                        white-space: normal !important; 
                        word-wrap: break-word !important; 
                        vertical-align: top !important; 
                        min-width: 100px;
                    }
                    /* Ensure the Bug IDs column has enough room */
                    th:last-child, td:last-child { min-width: 250px; }
                    </style>
                """, unsafe_allow_html=True)

                # --- QA & BUGS SECTION ---
                st.markdown('<div class="section-header">👩‍🔬 QA Activity & Bugs Logged</div>', unsafe_allow_html=True)
                q1, q2 = st.columns(2)

                with q1:
                    st.write("**Test Cases Created**")
                    qa_df = pd.DataFrame([{"QA Name": n, "Count": c} for n, c in qa_activity.items()])
                    st.write(qa_df.to_html(index=False), unsafe_allow_html=True)

                with q2:
                    st.write("**Bugs Created By**")
                    bugs_logged_df = pd.DataFrame([
                        {"Creator": c, "Total Bugs": len(l), "Bug IDs": ", ".join(l)} 
                        for c, l in bug_creators.items()
                    ])
                    
                    if not bugs_logged_df.empty:
                        ui_bug_df = bugs_logged_df.copy()
                        
                        # Format: BLUE ID (Clickable) + Plain Status
                        # Added a <span> wrapper to ensure the status stays grouped with the ID
                        ui_bug_df["Bug IDs"] = ui_bug_df["Bug IDs"].apply(
                            lambda x: ", ".join([
                                f'<span style="display:inline-block; margin-right:5px;">'
                                f'<a href="https://dev.azure.com/{ORG}/_workitems/edit/{i.split()[0]}" target="_blank">{i.split()[0]}</a>'
                                f' {" ".join(i.split()[1:]) if len(i.split()) > 1 else ""}</span>'
                                for i in x.split(", ")
                            ])
                        )
                        
                        # Displaying with full container width
                        st.write(ui_bug_df.to_html(escape=False, index=False, justify='left'), unsafe_allow_html=True)

                # ======================
                # PREPARE DATA FOR EXCEL
                # ======================
                
                # 1. KPI Sheet
                kpi_data = [
                    {"Metric": "Sprint Start Date", "Value": date_map_lookup.get(sel_path, {}).get("start", "N/A")},
                    {"Metric": "Sprint End Date", "Value": date_map_lookup.get(sel_path, {}).get("end", "N/A")},
                    {"Metric": "Total Stories", "Value": m_stats["ts"]},
                    {"Metric": "Stories Closed", "Value": m_stats["cs"]},
                    {"Metric": "Story Closure %", "Value": f"{s_perc}%"},
                    {"Metric": "Bugs Identified", "Value": m_stats["bi"]},
                    {"Metric": "Bugs Fixed", "Value": m_stats["bf"]},
                    {"Metric": "Bug Fix %", "Value": f"{b_perc}%"},
                    {"Metric": "Test Cases", "Value": m_stats["tc"]},
                    {"Metric": "Active Team Members", "Value": len(active_users)},
                    {"Metric": "Sprint Health", "Value": health_label}
                ]
                kpi_df = pd.DataFrame(kpi_data)

                # 2. Linkage Matrix (Cleaned for Excel)
                linkage_df_xl = pd.DataFrame(linkage_table).copy()
                if not linkage_df_xl.empty:
                    linkage_df_xl['ID'] = linkage_df_xl['ID'].str.replace(r'<[^>]*>', '', regex=True)
                    linkage_df_xl['Bugs'] = linkage_df_xl['Bugs'].str.replace(r'<[^>]*>', '', regex=True)

                # 3. Developer PRs (Cleaned for Excel)
                # Use .get() or check if dev_pr_map exists to avoid similar NameErrors
                pr_df_xl = pd.DataFrame([
                    {"Developer": d, "Items": len(v), "Work Items": ", ".join(list(v))}
                    for d, v in dev_pr_map.items()
                ]) if 'dev_pr_map' in locals() else pd.DataFrame()

                if not pr_df_xl.empty:
                    pr_df_xl['Work Items'] = pr_df_xl['Work Items'].str.replace(r'<[^>]*>', '', regex=True)

                # 4. Contributors
                contrib_df = pd.DataFrame([{"Contributor": k, **v} for k, v in contrib_data.items()])

                # 5. Resource Performance (FIX: Added check for res_stats existence)
                if 'res_stats' in locals():
                    res_matrix_df = pd.DataFrame([{"Resource": k, **v} for k, v in res_stats.items()])
                else:
                    res_matrix_df = pd.DataFrame()

                # ======================
                # EXCEL GENERATION
                # ======================


            # Create Excel file in memory using BytesIO
            output = BytesIO()

            # Use ExcelWriter to write the data into the memory
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                # Write your dataframes to different sheets (similar to your code)
                kpi_df.to_excel(writer, sheet_name="Summary_KPIs", index=False)
                
                if not linkage_df_xl.empty:
                    linkage_df_xl.to_excel(writer, sheet_name="UserStory_Bug_Linkage", index=False)
                
                if not contrib_df.empty:
                    contrib_df.to_excel(writer, sheet_name="Team_Contributors", index=False)
                
                if not pr_df_xl.empty:
                    pr_df_xl.to_excel(writer, sheet_name="Developer_PR_Activity", index=False)
                
                if not res_matrix_df.empty:
                    res_matrix_df.to_excel(writer, sheet_name="Resource_Performance", index=False)
                
                if not qa_df.empty:
                    qa_df.to_excel(writer, sheet_name="QA_Test_Cases", index=False)
                
                if not bugs_logged_df.empty:
                    bugs_logged_df.to_excel(writer, sheet_name="Bugs_Logged_By", index=False)

            # Finalize and get the processed data
            processed_data = output.getvalue()

            # Make sure to move the cursor back to the start of the BytesIO object
            output.seek(0)

            # Now you can pass the processed_data to the download button
            st.sidebar.download_button(
                label="📥 Download Excel Report",
                data=processed_data,
                file_name=f"Delivery_Matrix_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_btn"
            )
# Footer
st.markdown(
    "<div style='text-align:center;color:gray;font-size:12px;'>"
    "© 2026 Aventra Digital Pvt. Ltd. | Internal Use Only"
    "</div>",
    unsafe_allow_html=True
)