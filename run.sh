#!/bin/bash
set -e
python3 -m venv .venv
source .venv/bin/activate
pip install pysolr
pip install python-dotenv
python3 main.py