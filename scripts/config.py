import os

# SET THIS TO True to run ONLY "Live Auto"
# SET THIS TO False to run BOTH "Live Offline" and "Live Auto"
ONLY_LIVE_AUTO = False 

# Logic to determine the filter list
if os.environ.get("RUN_MODE") == "LIVE_AUTO" or ONLY_LIVE_AUTO:
    DEPLOYMENT_TYPES = ["Live Auto"]
else:
    DEPLOYMENT_TYPES = ["Live Offline", "Live Auto"]

# ==========================================
# TARGETED EMAIL EXECUTION FILTER
# ==========================================
# Use this list to run the script ONLY for specific user emails.
# - To run for specific users: Add their exact email addresses inside the brackets as strings, separated by commas.
#   Example: TARGET_EMAILS = ["dummy1@example.com", "dummy2@example.com", "dummy3@example.com"]
# - To run normally for ALL active users: Leave this list completely empty.
#   Example: TARGET_EMAILS = []
TARGET_EMAILS = ["dummy1@example.com", "dummy2@example.com", "dummy3@example.com"]
