"""
Runs both pipelines one after another. Just for convenience - you can
also run load_live_share.py and load_broker_summary.py separately.

Run it:
    python run_both.py
"""
import load_live_share
import load_broker_summary

print("========== LIVE SHARE ==========")
load_live_share.main()

print("\n========== BROKER SUMMARY ==========")
load_broker_summary.main()
