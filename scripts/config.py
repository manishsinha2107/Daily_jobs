import os

# SET THIS TO True to run ONLY "Live Auto"
# SET THIS TO False to run BOTH "Live Offline" and "Live Auto"
ONLY_LIVE_AUTO = True 

# Logic to determine the filter list
if os.environ.get("RUN_MODE") == "LIVE_AUTO" or ONLY_LIVE_AUTO:
    DEPLOYMENT_TYPES = ["Live Auto"]
else:
    DEPLOYMENT_TYPES = ["Live Offline", "Live Auto"]
