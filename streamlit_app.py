"""
Streamlit Community Cloud Entrypoint
Redirects to dashboard.py
"""
import os

entry = os.path.join(os.path.dirname(__file__), "dashboard.py")
with open(entry) as f:
    exec(f.read())
