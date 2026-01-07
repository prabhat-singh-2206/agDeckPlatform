import requests
import urllib.parse
import pandas as pd
from collections import defaultdict
import streamlit as st
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

# ==================================================
# CONSTANTS
# ==================================================
ORG = "lloydsregistergroup"

STORY_TYPES = ["User Story", "Requirement", "Product Backlog Item"]

PERIOD_TO_DAYS = {
    "30 Days": 30,
    "60 Days": 60,
    "90 Days": 90,
    "180 Days": 180,
    "365 Days": 365
}

# ==================================================
# DEFAULT SUMMARY
# ==================================================
def default_summary():
    return {
        "Stories": 0,
        "Bugs": 0,
        "TestCases": 0,
        "StoryPoints": 0,
        "Items": []
    }

# ==================================================
# PERFORMANCE LAYER: PARALLEL HISTORY FETCH
# ==================================================
def get_contributors_from_history(wi_id, auth):
    """Fetch all unique users assigned to a work item through history."""
    url = f"https://dev.azure.com/{ORG}/_apis/wit/workitems/{wi_id}/revisions?api-version=7.0"
    try:
        r = requests.get(url, auth=auth, timeout=10)
        if r.status_code != 200:
            return set()

        users = set()
        for rev in r.json().get("value", []):
            assigned_to = rev.get("fields", {}).get("System.AssignedTo")
            if isinstance(assigned_to, dict):
                users.add(assigned_to.get("displayName"))
        return users
    except Exception:
        return set()

# ==================================================
# DATA LAYER: OPTIMIZED MATRIX GENERATION
# ==================================================
@st.cache_data(ttl=3600)
def get_resource_matrix(_auth, project, area_path, period_label):
    days = PERIOD_TO_DAYS.get(period_label, 30)

    wiql = f"""
    SELECT [System.Id]
    FROM WorkItems
    WHERE [System.TeamProject] = '{project}'
      AND [System.AreaPath] UNDER '{area_path}'
      AND [System.WorkItemType] IN ('Bug','User Story','Requirement','Product Backlog Item')
      AND [System.ChangedDate] >= @today - {days}
    """

    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/wiql?api-version=7.0"
    r = requests.post(url, json={"query": wiql}, auth=_auth)

    if r.status_code != 200 or not r.json().get("workItems"):
        return pd.DataFrame(), {}

    wi_ids = [item["id"] for item in r.json()["workItems"]]

    # Fetch work items in batch
    items_details = _fetch_work_items(wi_ids, _auth)

    # Fetch histories in parallel
    with ThreadPoolExecutor(max_workers=15) as executor:
        history_map = dict(
            executor.map(
                lambda wid: (wid, get_contributors_from_history(wid, _auth)),
                wi_ids
            )
        )

    summary = defaultdict(default_summary)

    for wi_id, contributors in history_map.items():
        item = items_details.get(wi_id)
        if not item:
            continue

        for user in contributors:
            if user == "Unassigned":
                continue

            if item["type"] in STORY_TYPES:
                summary[user]["Stories"] += 1
                summary[user]["StoryPoints"] += item.get("story_points", 0)
            elif item["type"] == "Bug":
                summary[user]["Bugs"] += 1

            summary[user]["Items"].append({
                "ID": item["id"],
                "Type": item["type"],
                "State": item["state"],
                "StoryPoints": item.get("story_points", 0),
                "Title": item.get("title", "")
            })

    # Fetch test cases once
    tc_counts = get_all_test_cases_by_user(_auth, project, area_path, days)
    for user, count in tc_counts.items():
        summary[user]["TestCases"] = count

    rows = []
    for user, s in summary.items():
        rows.append({
            "Resource": user,
            "Stories": s["Stories"],
            "Bugs": s["Bugs"],
            "TestCases": s["TestCases"],
            "StoryPoints": s["StoryPoints"]
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df, {}

    df["Total Work Items"] = df["Stories"] + df["Bugs"] + df["TestCases"]
    return df.sort_values(
        ["StoryPoints", "Total Work Items"],
        ascending=False
    ), summary

# ==================================================
# HELPERS
# ==================================================
def _fetch_work_items(ids, auth):
    result = {}
    for i in range(0, len(ids), 200):
        batch = ids[i:i + 200]
        url = (
            f"https://dev.azure.com/{ORG}/_apis/wit/workitems"
            f"?ids={','.join(map(str, batch))}&api-version=7.0"
        )
        r = requests.get(url, auth=auth)
        if r.status_code == 200:
            for item in r.json().get("value", []):
                f = item["fields"]
                result[item["id"]] = {
                    "id": item["id"],
                    "type": f.get("System.WorkItemType"),
                    "state": f.get("System.State"),
                    "title": f.get("System.Title"),
                    "story_points": f.get(
                        "Microsoft.VSTS.Scheduling.StoryPoints", 0
                    )
                }
    return result

@st.cache_data(ttl=3600)
def get_all_test_cases_by_user(_auth, project, area_path, days):
    wiql = f"""
    SELECT [System.Id]
    FROM WorkItems
    WHERE [System.TeamProject] = '{project}'
      AND [System.AreaPath] UNDER '{area_path}'
      AND [System.WorkItemType] = 'Test Case'
      AND [System.ChangedDate] >= @today - {days}
    """

    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/wiql?api-version=7.0"
    r = requests.post(url, json={"query": wiql}, auth=_auth)

    counts = defaultdict(int)
    if r.status_code == 200:
        ids = [w["id"] for w in r.json().get("workItems", [])]
        for i in range(0, len(ids), 200):
            batch = ids[i:i + 200]
            b_url = (
                f"https://dev.azure.com/{ORG}/_apis/wit/workitems"
                f"?ids={','.join(map(str, batch))}"
                f"&fields=System.AssignedTo&api-version=7.0"
            )
            res = requests.get(b_url, auth=_auth).json()
            for item in res.get("value", []):
                user = item["fields"].get("System.AssignedTo", {}).get(
                    "displayName", "Unassigned"
                )
                if user != "Unassigned":
                    counts[user] += 1
    return counts

# ==================================================
# BASIC ADO HELPERS
# ==================================================
def get_projects(auth):
    url = f"https://dev.azure.com/{ORG}/_apis/projects?api-version=7.0"
    r = requests.get(url, auth=auth)
    return [p["name"] for p in r.json().get("value", [])] if r.status_code == 200 else []

def get_area_paths(project, auth):
    url = (
        f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}"
        f"/_apis/wit/classificationnodes/areas?$depth=2&api-version=7.0"
    )
    r = requests.get(url, auth=auth)
    paths = []

    def walk(node, parent=""):
        path = f"{parent}\\{node['name']}" if parent else node["name"]
        paths.append(path)
        for c in node.get("children", []):
            walk(c, path)

    if r.status_code == 200:
        walk(r.json())
    return paths

# ==================================================
# UI RENDERER
# ==================================================
def render_resource_view(auth):
    st.title("👤 Resource Contribution Matrix")

    col1, col2 = st.columns(2)
    with col1:
        project = st.selectbox("Project", get_projects(auth))
    with col2:
        area_path = st.selectbox("Area Path", get_area_paths(project, auth))

    period = st.selectbox(
        "📆 Contribution Lookback Period",
        list(PERIOD_TO_DAYS.keys())
    )

    if st.button("🚀 Analyze Contributions", use_container_width=True):
        with st.spinner("Analyzing history..."):
            df, summary = get_resource_matrix(auth, project, area_path, period)
            st.session_state.matrix_df = df
            st.session_state.matrix_summary = summary

    if "matrix_df" in st.session_state and not st.session_state.matrix_df.empty:
        df = st.session_state.matrix_df
        summary = st.session_state.matrix_summary

        st.subheader("Performance Summary")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.divider()

        target_user = st.selectbox(
            "🔍 Detailed Activity Log (Select User)",
            df["Resource"].tolist(),
            key="user_selector"
        )

        if target_user in summary:
            user_data = summary[target_user]
            m1, m2, m3 = st.columns(3)
            m1.metric("Stories Worked", user_data["Stories"])
            m2.metric("Bugs Worked", user_data["Bugs"])
            m3.metric("Total Story Points", user_data["StoryPoints"])

            export_data = [{
                "ID": item["ID"],
                "Work Item Type": item["Type"],
                "Title": item["Title"],
                "State": item["State"],
                "Story Points": item["StoryPoints"]
            } for item in user_data["Items"]]

            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                summary_df = pd.DataFrame({
                    "Metric": [
                        "Resource Name",
                        "Stories",
                        "Bugs",
                        "Total Story Points"
                    ],
                    "Value": [
                        target_user,
                        user_data["Stories"],
                        user_data["Bugs"],
                        user_data["StoryPoints"]
                    ]
                })
                summary_df.to_excel(
                    writer,
                    index=False,
                    sheet_name="User_Activity_Log",
                    startrow=0
                )
                pd.DataFrame(export_data).to_excel(
                    writer,
                    index=False,
                    sheet_name="User_Activity_Log",
                    startrow=6
                )

            st.download_button(
                f"📥 Download Activity Log for {target_user}",
                data=buffer.getvalue(),
                file_name=f"Activity_Log_{target_user}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            log_df = pd.DataFrame(user_data["Items"])
            if not log_df.empty:
                st.dataframe(
                    log_df[["ID", "Type", "Title", "State"]],
                    use_container_width=True,
                    hide_index=True
                )

    elif "matrix_df" in st.session_state:
        st.warning("No activity found for the selected filters.")