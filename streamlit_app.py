"""
Streamlit Community Cloud Entrypoint
Redirects to dashboard.py
"""
import os
import runpy

entry = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py")
runpy.run_path(entry, run_name="__main__")
