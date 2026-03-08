# ===============================
# C1 — Crypto snapshot ingestion
# ===============================

import requests
import json
import time
from datetime import datetime

# -------------------------------
# Config
# -------------------------------
URL = "https://api.coingecko.com/api/v3/coins/markets"
landing_path = "/Volumes/p1_crypto/bronze_layer/crypto_files_volume/raw"

API_KEY = "CG-Y74CNS8iRijPjxtuqBrTm5bW"

#NEW → page cap
MAX_PAGES = 13

# -------------------------------
# Snapshot fetch function
# -------------------------------
def fetch_crypto_snapshot():

    all_data = []
    page = 1
    per_page = 250

    header = {
        "x-cg-demo-api-key": API_KEY
    }

    #looping only till 13 pages to get the top coins only because of the rate_limit, manaing hourly schedule, monthly api limit
    while page <= MAX_PAGES:

        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false"
        }

        response = requests.get(URL, headers=header, params=params)

        # Rate limit handling
        if response.status_code == 429:
            print("Rate limited. Sleeping 5 seconds...")
            time.sleep(5)
            continue

        if response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text}")

        page_data = response.json()

        # Stop when empty page (safety)
        if not page_data:
            break

        all_data.extend(page_data)

        print(f"Fetched page {page} → {len(page_data)} rows")

        page += 1

        #just to be safe
        time.sleep(1)

    # -------------------------------
    # Snapshot envelope
    # ---------/Volumes/p1_crypto/bronze_layer/crypto_files_volume/raw/crypto_snapshot_20260226_073109.json----------------------
    snapshot = {
        "ingestion_time": datetime.utcnow().isoformat(),
        "record_count": len(all_data),
        "data": all_data
    }

    # -------------------------------
    # Save snapshot
    # -------------------------------
    file_name = f"{landing_path}/crypto_snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    dbutils.fs.put(file_name, json.dumps(snapshot), overwrite=False)

    print(f"Snapshot saved → {file_name}")
    print(f"Total records → {len(all_data)}")


# calling the function
fetch_crypto_snapshot()