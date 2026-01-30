import pandas as pd
import re
import unicodedata
import requests
import json
import io
import gc
import time
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# --- Configuration ---

BUCKET_NAME = "diethub_calls"
PROJECT_ID = "data-analysis-450711"
DATASET_ID = "Crm_Diethub"
TABLE_ID = "Crm_Calls"
# Added these because run_bigquery_merge relies on them
MAIN_TABLE_ID = "Crm_Calls"
STAGING_TABLE_ID = "Crm_Calls_Staging" 
BITRIX_WEBHOOK = "https://diet-hub.bitrix24.com/rest/625003/f2ijr5q7sa4w5z6g/"


def normalize_arabic(text):
    if pd.isna(text): return ""
    if text is False: return ""
    
    arabic_digits = {'٠':'0', '١':'1', '٢':'2', '٣':'3', '٤':'4',
                     '٥':'5', '٦':'6', '٧':'7', '٨':'8', '٩':'9'}
    text = str(text)
    for arabic, western in arabic_digits.items():
        text = text.replace(arabic, western)
    text = re.sub(r'[\u0600-\u06FF]', '', text)
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text.strip()


def normalize_lists(val):
    """
    Fix for 'cannot mix list and non-list':
    Converts list ['A', 'B'] -> string "A, B"
    """
    if isinstance(val, list):
        return ", ".join([str(v) for v in val if v])
    if val is False: 
        return None
    return val


def extract_last_10_digits(text):
    """
    Helper function:
    1. Removes all non-digit characters.
    2. Returns the LAST 10 digits if the total length is >= 10.
    """
    if pd.isna(text) or text == '':
        return None
    
    text = normalize_arabic(str(text))
    
    if ',' in text:
        text = text.split(',')[0]
    elif ' - ' in text and len(text) > 20: 
        text = text.split(' - ')[0]

    digits = re.sub(r'\D', '', text) 
    
    if len(digits) >= 10:
        return digits[-10:]
        
    return None


def fix_date_format(date_str):
    if pd.isna(date_str) or str(date_str).strip() in ['', 'nan', 'None', 'NaT', 'False']:
        return None

    date_str = normalize_arabic(str(date_str)).strip()

    # Remove Timezone Offset (e.g., +03:00) at the end
    date_str = re.sub(r'[+-]\d{2}:?\d{2}$', '', date_str) 

    # Clean T/Z and milliseconds
    date_str = re.sub(r'[TZ]', ' ', date_str) 
    date_str = re.sub(r'\.\d+', '', date_str)  
    date_str = date_str.strip()

    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%d/%m/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%Y%m%d',
    ]

    for fmt in formats_to_try:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d') 
        except ValueError:
            continue

    try:
        dt = pd.to_datetime(date_str, errors='coerce', dayfirst=True)
        if pd.notnull(dt):
            return dt.strftime('%Y-%m-%d')
    except:
        pass

    return None


def convert_to_strict_date(series):
    clean_series = series.apply(lambda x: normalize_arabic(str(x)) if pd.notnull(x) else None)
    
    dt_series = pd.to_datetime(clean_series, errors='coerce', dayfirst=True, utc=True)
    
    mask_fail = dt_series.isna() & clean_series.notnull()
    if mask_fail.any():
        try:
            dt_series.loc[mask_fail] = pd.to_datetime(
                clean_series.loc[mask_fail], 
                format='%Y-%d-%m', 
                errors='coerce', 
                utc=True
            )
        except:
            pass

    if pd.api.types.is_datetime64tz_dtype(dt_series):
        dt_series = dt_series.dt.tz_localize(None)

    return dt_series.dt.strftime('%Y-%m-%d')


def fetch_all_bitrix_users():
    all_users = []
    start = 0

    while True:
        params = {"start": start}
        res = requests.post(f"{BITRIX_WEBHOOK}/user.get", json=params, timeout=60).json()

        batch = res.get("result", [])
        if not batch:
            break

        all_users.extend(batch)

        if "next" in res:
            start = res["next"]
        else:
            break

    return all_users


def fetch_sip_lines_map():
    sip_map = {}
    start = 0
    print("Fetching Voximplant SIP lines...")

    while True:
        params = {"start": start}
        try:
            res = requests.post(f"{BITRIX_WEBHOOK}/voximplant.sip.get", json=params, timeout=60).json()
            batch = res.get("result", [])
            if not batch:
                break

            for item in batch:
                reg_id = item.get('REG_ID')
                title = item.get('TITLE')
                if reg_id and title:
                    key = f"reg{reg_id}"
                    sip_map[key] = title

            if "next" in res:
                start = res["next"]
                time.sleep(0.5)
            else:
                break
        except Exception as e:
            print(f"Error fetching SIP lines: {e}")
            break

    print(f"Mapped {len(sip_map)} SIP lines.")
    return sip_map


def get_bitrix_maps():
    print("Fetching Maps...")
    maps = {'portal_name': {},'portal_user': {},'call_type': {}}

    try:
        users = fetch_all_bitrix_users() 
        for u in users:
            user_id = str(u.get("ID"))
            first = u.get("NAME", "")
            last = u.get("LAST_NAME", "")
            full_name = f"{first} {last}".strip()
            maps["portal_user"][user_id] = normalize_arabic(full_name)
        
        maps["portal_name"] = fetch_sip_lines_map()
        maps["call_type"] = {'0': 'Outgoing', '1': 'Incoming'}

    except Exception as e:
        print(f" Map Error: {e}")
    return maps


def process_source(row):
    source_val = row.get('SOURCE_ID', row.get('source', ''))
    contact_val = row.get('CONTACT_SOURCE', '')
    source_str = normalize_arabic(source_val) if not pd.isna(source_val) else ''
    contact_str = normalize_arabic(contact_val) if not pd.isna(contact_val) else ''
    
    valid_sources = ['Instagram - Diet Hub', 'Facebook - Diet Hub', 'Instagram - Jidalur',
                     'Facebook - Jidalur', 'Facebook - Beltix', 'Instagram - Beltix', 
                     'IG Lead generation', 'Lead generation']

    if source_str in ['Whatsapp - Marketing', 'Whatsapp - Mou']: return source_str
    if re.match(r'(?i)^Whatsapp($|\s[^-]| -[^-])', source_str): return 'WhatsApp'
    if source_str in ['Instant form', 'Lead generation', 'IG lead generation']: return source_str
    if source_str in valid_sources: return source_str
    if source_str == 'CRM form': return 'Others'
    if source_str == 'Callback': return 'Call'
    if source_str == 'IG to site': return 'Ig to site'
    if source_str == 'FB To Website': return 'FB To Website'
    if contact_str in valid_sources: return contact_str
    if re.match(r'(?i)^Sales\s*Whatsapp', source_str): return 'WhatsApp'
    return source_str if source_str and source_str != 'nan' else 'Others'


def get_best_phone(row):
    # --- PRIORITY 1: The Phone Column ---
    phone_col_val = row.get('UF_CRM_1725452218751')
    
    # Clean it
    clean_phone = extract_last_10_digits(phone_col_val)
    
    if clean_phone:
        return clean_phone 

    # --- PRIORITY 2: The Title Column ---
    title_val = row.get('TITLE')
    
    # Fixed: Use extract_last_10_digits instead of undefined extract_phone_from_title
    clean_phone_title = extract_last_10_digits(title_val)
    
    if clean_phone_title:
        return clean_phone_title

    # --- Final Fallback ---
    return None


def fill_date_fallback(row):
    val = row.get('UF_CRM_1736158245296') # code of Creation Date
    if pd.isna(val) or val is False or str(val).strip() == '':
        return row.get('DATE_CREATE')
    return val


def run_bigquery_merge(client, df_columns):
    """
    Executes a Smart SQL MERGE.
    """
    full_main_ref = f"{PROJECT_ID}.{DATASET_ID}.{MAIN_TABLE_ID}"
    full_staging_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"

    print(f"Fetching schema for {full_main_ref} to ensure type safety...")
    
    try:
        main_table = client.get_table(full_main_ref)
        schema_map = {field.name.upper(): field.field_type for field in main_table.schema}
    except Exception as e:
        print(f"Could not fetch main table schema: {e}")
        schema_map = {}

    update_list = []
    insert_cols = []
    insert_vals = []

    for col in df_columns:
        col_upper = col.upper()
        target_type = schema_map.get(col_upper)
        
        if target_type == 'STRING':
            src_val = f"CAST(S.{col} AS STRING)"
        elif target_type in ['TIMESTAMP', 'DATE', 'DATETIME']:
            src_val = f"S.{col}" 
        else:
            src_val = f"S.{col}"

        if col != 'ID':
            update_list.append(f"T.{col} = {src_val}")
        
        insert_cols.append(col)
        insert_vals.append(src_val)

    update_clause = ", ".join(update_list)
    insert_cols_str = ", ".join(insert_cols)
    insert_vals_str = ", ".join(insert_vals)

    sql = f"""
        MERGE `{full_main_ref}` T
        USING `{full_staging_ref}` S
        ON T.ID = S.ID
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols_str}) VALUES ({insert_vals_str})
    """

    print(" Running Smart BigQuery MERGE (with Auto-Cast)...")
    
    try:
        query_job = client.query(sql)
        query_job.result()
        print(" Merge Success! Data synchronized with correct types.")
    except Exception as e:
        print(f"Merge Failed: {e}")
        print(f"DEBUG SQL: {sql[:500]} ...") 
        raise e
    