# iteration_utils.py

import requests
import urllib.parse
import re
import streamlit as st
from ado_client import ORG, AUTH


@st.cache_data(ttl=3600, show_spinner=False)
def get_iteration_paths_with_dates(project):
    if not project:
        return [], {}

    url = (
        f"https://dev.azure.com/{ORG}/"
        f"{urllib.parse.quote(project)}/"
        "_apis/wit/classificationnodes/iterations?$depth=10&api-version=7.0"
    )

    r = requests.get(url, auth=AUTH)
    r.raise_for_status()

    data = r.json()
    paths = []
    dates = {}

    def walk(node):
        path = node.get("path")

        if path and path.lower() != project.lower():
            # remove only 'Iteration'
            path = path.replace("Iteration", "").replace("\\\\", "\\").strip("\\")

            paths.append(path)

            attr = node.get("attributes", {})
            if attr.get("startDate") and attr.get("finishDate"):
                dates[path] = {
                    "start": attr["startDate"][:10],
                    "end": attr["finishDate"][:10]
                }

        for child in node.get("children", []):
            walk(child)

    walk(data)

    # Natural sorting (Sprint 2 < Sprint 10)
    paths = sorted(
        set(paths),
        key=lambda x: [int(t) if t.isdigit() else t.lower()
                       for t in re.split(r"(\d+)", x)]
    )

    return paths, dates
