#!/bin/bash
# Run the Zombie Fund Screener on Mac/Linux.
cd "$(dirname "$0")"
pip install -r requirements.txt
streamlit run Today.py
