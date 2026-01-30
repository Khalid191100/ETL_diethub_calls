import pandas as pd
import requests
import json
import re
import unicodedata
import pytz 
from datetime import datetime, timedelta
import time
import os  
from google.cloud import storage

BITRIX_WEBHOOK = "https://diet-hub.bitrix24.com/rest/625003/f2ijr5q7sa4w5z6g/"
METHOD = "voximplant.statistic.get"


def fetch_deals_from_bitrix(days=1):

    since_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    print(f"Fetching Calls Modified ( Updated) since {since_date}...")
    
    all_deals = []
    start = 0
    
    while True:
        params = {
            "filter": {">=Call_Start_Date": since_date},
            "select": ["*"], 

            "order": {"Call_Start_Date": "ASC"}, 
            "start": start
        }

        # --- Retry Logic Loop ---
        max_retries = 30
        attempt = 0
        success = False
        
        while attempt < max_retries:
            try:
                response = requests.post(f"{BITRIX_WEBHOOK}/{METHOD}", json=params)
                
                
                if response.status_code == 429:
                    print(f"  Rate Limit hit (429). Cooling down for 10 seconds...")
                    time.sleep(10) 
                    attempt += 1
                    continue 
                
                response.raise_for_status()
                
                data = response.json()
                success = True
                break 
                
            except requests.exceptions.RequestException as e:
                print(f"  Network error: {e}. Retrying in 5s...")
                time.sleep(5)
                attempt += 1

        if not success:
            print(f" Failed to fetch batch at start {start} after {max_retries} attempts. Stopping.")
            break
            
        # --- Process Data ---
        batch = data.get('result', [])
        if not batch:
            break
            
        all_deals.extend(batch)
        print(f"   Fetched batch (start={start})... Total collected: {len(all_deals)}")
        
        # Pagination Check
        if 'next' in data and len(batch) >= 50:
            start = data['next']
            
            time.sleep(0.5) 
        else:
            break
            
    df = pd.DataFrame(all_deals)
    
    
    if not df.empty:
        df = df.drop_duplicates(subset='ID', keep='last')
        
    print(f"Total Unique Deals: {len(df)}")
    return df
    



