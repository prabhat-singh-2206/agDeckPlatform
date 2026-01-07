# ado_client.py
import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
import urllib.parse

ORG = "lloydsregistergroup"
#PAT = ""
PAT = st.secrets["AZURE_DEVOPS_PAT"]

AUTH = HTTPBasicAuth("", PAT)
HEADERS = {"Content-Type": "application/json"}

STORY_TYPES = ["User Story", "Requirement", "Product Backlog Item"]

@st.cache_data(ttl=3600)
def get_all_projects():
    url = f"https://dev.azure.com/{ORG}/_apis/projects?api-version=6.0"
    r = requests.get(url, auth=AUTH)
    return sorted(p["name"] for p in r.json().get("value", []))

@st.cache_data(ttl=3600)
def get_area_paths(project):
    url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/classificationNodes/Areas?$depth=5&api-version=7.0"
    r = requests.get(url, auth=AUTH)

    paths = []

    def walk(node, prefix=""):
        name = node.get("name", "")
        full = f"{prefix}\\{name}" if prefix else name
        paths.append(full)
        for c in node.get("children", []):
            walk(c, full)

    walk(r.json())
    return sorted(paths)

@st.cache_data(ttl=1800)
def fetch_work_items(project, area):
    query = f"SELECT [System.Id] FROM WorkItems WHERE [System.AreaPath] UNDER '{area}'"

    wiql_url = f"https://dev.azure.com/{ORG}/{urllib.parse.quote(project)}/_apis/wit/wiql?api-version=7.0"
    r = requests.post(wiql_url, json={"query": query}, auth=AUTH)
    ids = [i["id"] for i in r.json().get("workItems", [])]

    if not ids:
        return {}

    data = {}
    for i in range(0, len(ids), 200):
        batch = ids[i:i+200]
        url = f"https://dev.azure.com/{ORG}/_apis/wit/workitems?ids={','.join(map(str,batch))}&api-version=7.0"
        r = requests.get(url, auth=AUTH)

        for wi in r.json().get("value", []):
            f = wi["fields"]
            data[wi["id"]] = {
                "id": wi["id"],
                "type": f.get("System.WorkItemType"),
                "title": f.get("System.Title"),
                "state": f.get("System.State"),
                "assigned_to": f.get("System.AssignedTo", {}).get("displayName", "Unassigned"),
                "story_points": f.get("Microsoft.VSTS.Scheduling.StoryPoints", 0)
            }

    return data
