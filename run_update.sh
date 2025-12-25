#!/bin/bash
cd /Users/khangnguyen/Documents/python/nba_project
source venv/bin/activate
python -m update_recent >> logs/update.log 2>&1
python sync_to_bigquery.py