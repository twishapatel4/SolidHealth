import json
from duckdb import df
from duckdb import df
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
import hashlib
import uuid
import re
from collections import Counter
import sys
import base64

def hash_binary_data(data):
    if not data:
        return None
    try:
        decoded = base64.b64decode(data)
        return hashlib.sha256(decoded).hexdigest()
    except:
        return None
# --- UTILS ---
def normalize_string(s):
    if not s or s == "N/A": return "na"
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def clean_id(raw_id):
    if raw_id is None or pd.isna(raw_id): return "MISSING"
    return str(raw_id).split('/')[-1].strip()

def get_val(obj, path, default=None):
    components = path.split('.')
    for part in components:
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit():
            try: obj = obj[int(part)]
            except: return default
        else:
            return default
    return obj if obj is not None else default

def normalize_system(system):
    if not system:
        return "unknown"

    s = system.lower()

    if "aetna" in s or "cigna" in s:
        return "payer_system"

    if "mrn" in s:
        return "mrn"

    if "ssn" in s:
        return "ssn"

    return s

def normalize_identifier(identifier):
    if not identifier:
        return None

    # Handle list of identifiers
    if isinstance(identifier, list):
        return [normalize_identifier(i) for i in identifier if i]

    if not isinstance(identifier, dict):
        return None

    return {
        # Keep semantic meaning (type)
        "type": normalize_string(get_val(identifier, "type.coding.0.code") or "unknown"),

        # Normalize system → remove platform dependency
        "system": normalize_system(identifier.get("system")),

        # Mask value → remove platform-specific IDs
        "value": "GENERIC_ID"
    }

# --- CLINICAL HASHING LOGIC ---
def get_clinical_essence(obj):
    noise = {
        'id', 'meta', 'text', 'reference', 'lastUpdated', 'versionId', 
        'url', 'fullUrl'
    }

    # --- 🔴 HANDLE BINARY ---
    if isinstance(obj, dict) and obj.get("resourceType") == "Binary":
        return {
            "resourceType": "Binary",
            "contentType": obj.get("contentType"),
            "data_hash": hash_binary_data(obj.get("data"))
        }

    # --- 🔵 HANDLE DOCUMENTREFERENCE ---
    if isinstance(obj, dict) and obj.get("resourceType") == "DocumentReference":
        attachment = get_val(obj, "content.0.attachment")

        return {
            "resourceType": "DocumentReference",
            "type": normalize_string(get_val(obj, "type.coding.0.code")),
            "category": normalize_string(get_val(obj, "category.0.coding.0.code")),
            "date": str(obj.get("date", ""))[:16],
            "data_hash": hash_binary_data(attachment.get("data")) if attachment and attachment.get("data") else None,
            "url": normalize_string(attachment.get("url")) if attachment and attachment.get("url") else None,
            "contentType": attachment.get("contentType") if attachment else None
        }
    elif isinstance(obj, dict):
        cleaned = {}

        for k, v in obj.items():
            if k in noise:
                continue
            if k == "identifier":
                cleaned[k] = normalize_identifier(v)
                continue
            if k == "system":
                cleaned[k] = normalize_system(v)
                continue
            cleaned[k] = get_clinical_essence(v)

        return {k: v for k, v in cleaned.items() if v not in [None, "", [], {}]}
    elif isinstance(obj, list):
        return sorted(
            [get_clinical_essence(x) for x in obj if x is not None],
            key=lambda x: str(x)
        )
    elif isinstance(obj, (int, float)):
        return float(obj)
    elif isinstance(obj, str):
        return obj.strip().lower()
    return obj

def generate_clinical_hash(obj):
    essence = get_clinical_essence(obj)
    serialized = json.dumps(essence, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized).hexdigest()

def deep_diff(d1, d2, path=""):
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        return f"{d1} ➔ {d2}"
    diffs = []
    keys = set(d1.keys()) | set(d2.keys())
    for k in keys:
        if k in ['data', 'attachment']: continue
        v1, v2 = d1.get(k), d2.get(k)
        if v1 != v2:
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub = deep_diff(v1, v2, f"{path}{k}.")
                if sub: diffs.append(sub)
            else:
                diffs.append(f"{path}{k}: {v1} ➔ {v2}")
    return ", ".join(diffs)

# --- STAGE 1: FEATURE EXTRACTION (Deterministic Logic) ---
def extract_features(data, source_name):
    temp_list = []
    file_stats = Counter()

    for item in data:
        res_type = item.get('resourceType')
        if res_type == "Provenance": continue
        
        file_stats[res_type] += 1
        internal_id = clean_id(item.get('id'))
        raw_ref = (item.get('subject', {}).get('reference') or 
                   item.get('patient', {}).get('reference') or 
                   item.get('subject', {}).get('display') or "")
        patient_ref = clean_id(raw_ref)
        
        # Identity Logic (Code)
        if res_type == "Binary":
            code = internal_id # Use the ID for Binaries to keep them unique
        else:
            code = (get_val(item, "code.coding.0.code") or 
                    get_val(item, "vaccineCode.coding.0.code") or 
                    get_val(item, "type.0.coding.0.code") or 
                    get_val(item, "type.coding.0.code") or 
                    get_val(item, "category.0.coding.0.code") or 
                    get_val(item, "category.1.coding.0.code") or 
                    get_val(item, "name") or    
                    get_val(item, "class.code") or 
                    get_val(item, "medicationCodeableConcept.coding.0.code") or 
                    clean_id(get_val(item, "medicationReference.reference")) or 
                    "NOCODE")
        
        # Identity Logic (Date)
        date_raw = (get_val(item, "effectiveDateTime") or 
                    get_val(item, "performedDateTime") or 
                    get_val(item, "recordedDate") or 
                    get_val(item, "onsetDateTime") or
                    get_val(item, "authoredOn") or 
                    get_val(item, "occurrenceDateTime") or 
                    get_val(item, "period.start") or 
                    get_val(item, "date") or 
                    get_val(item, "meta.lastUpdated") or "NODATE")
        
        date_key = str(date_raw)[:16].upper()
        essence = get_clinical_essence(item)

        temp_list.append({
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "code": code,             # <--- ADD THIS
            "date_key": date_key, 
            # "clinical_id": f"{code}|{date_key}",
            "clinical_id": f"{res_type}|{code}|{date_key}|{patient_ref}",
            "payload_hash": generate_clinical_hash(item),
            "essence": essence,
            "essence_str": str(essence),
            "clinical_val": str(get_val(item, "valueQuantity.value") or get_val(item, "status") or "N/A")
        })

    if not temp_list: return [], file_stats

    # --- THE SORTING FIX (Ensures 3 MODIFIED instead of 20) ---
    # df = pd.DataFrame(temp_list)
    # df = df.sort_values(by=['resourceType', 'clinical_id', 'essence_str'])
    # # df['seq'] = df.groupby(['resourceType', 'clinical_id']).cumcount() + 1
    # # df['fingerprint_id'] = df['resourceType'] + "|" + df['clinical_id'] + "|" + df['seq'].astype(str)
    # # df['fingerprint_id'] = df['resourceType'] + "|" + df['clinical_id'] + "|" + df['payload_hash']
    # def soft_hash(s):
    #     return hashlib.md5(s.encode()).hexdigest()[:6]

    # df['soft_id'] = df['essence_str'].apply(soft_hash)

    # # df['fingerprint_id'] = (
    # #     df['resourceType'] + "|" +
    # #     df['clinical_id'] + "|" +
    # #     df['soft_id']
    # # )
    # df['fingerprint_id'] = (
    # df['resourceType'] + "|" +
    # df['clinical_id']
    # )
    return df.to_dict('records'), file_stats

# --- IDENTITY RESOLUTION ---
def get_patient_matches(df_patients):
    if len(df_patients) < 2: return pd.DataFrame()
    settings = {"link_type": "dedupe_only", "unique_id_column_name": "linkage_id",
                "comparisons": [{"output_column_name": "internal_id", "comparison_levels": [
                    {"sql_condition": "internal_id_l = internal_id_r", "m_probability": 0.99, "u_probability": 0.01},
                    {"sql_condition": "ELSE", "m_probability": 0.01, "u_probability": 0.99}]}]}
    return Linker(df_patients, settings, DuckDBAPI()).inference.predict(threshold_match_probability=0.9).as_pandas_dataframe()

def create_global_id_map(df_patients, df_matches):
    G = nx.Graph()
    l_to_i = dict(zip(df_patients['linkage_id'], df_patients['internal_id']))
    for lid in df_patients['linkage_id']: G.add_node(lid)
    for _, row in df_matches.iterrows(): G.add_edge(row['linkage_id_l'], row['linkage_id_r'])
    mapping = {}
    for cluster in nx.connected_components(G):
        # unique_global_id = "PATIENT-ELIJAH-FISHER"
        unique_global_id =  f"GID-{str(uuid.uuid4())[:8].upper()}"
        for lid in cluster: mapping[l_to_i[lid]] = unique_global_id
    return mapping

def normalize_date(d):
    if not d or d == "NODATE": return "NODATE"
    try:
        # Convert to UTC and then format
        return pd.to_datetime(d, utc=True).strftime('%Y-%m-%dT%H:%M')
    except:
        return str(d)[:16]
    
# --- MAIN ---
def main(file1, file2):
    # Load raw data first to check patient identity
    f1_data = json.load(open(file1))
    f2_data = json.load(open(file2))

    # --- STEP 1: PATIENT GATEKEEPER ---
    def get_patient_fingerprint(data):
        p = next((i for i in data if i.get('resourceType') == 'Patient'), None)
        if not p: return None
        # Identity is Name + BirthDate
        return f"{normalize_string(get_val(p, 'name.0.family'))}|{normalize_string(get_val(p, 'name.0.given.0'))}|{get_val(p, 'birthDate')}"

    id1 = get_patient_fingerprint(f1_data)
    id2 = get_patient_fingerprint(f2_data)

    if id1 != id2:
        print(f"❌ ABORT: Patient identity mismatch.")
        print(f"File 1 Patient: {id1}")
        print(f"File 2 Patient: {id2}")
        return

    print("✅ Patient Match Confirmed. Proceeding with Full Clinical Audit...")

    # --- STEP 2: FULL CLINICAL COMPARISON ---
    raw1, stats1 = extract_features(f1_data, "P1")
    raw2, stats2 = extract_features(f2_data, "P2")

    df_all = pd.concat([pd.DataFrame(raw1), pd.DataFrame(raw2)])
    id_map = create_global_id_map(df_all[df_all['resourceType']=='Patient'], get_patient_matches(df_all[df_all['resourceType']=='Patient']))
    default_gid = list(id_map.values())[0] if id_map else "GLOBAL-SYSTEM"

    def build_index(rows):
        df = pd.DataFrame(rows)
        def get_owner(row):
            gid = id_map.get(row['patient_ref'])
            if gid: return gid
            if row['resourceType'] in ["Location", "Organization", "Practitioner", "Medication","Binary","DiagnosticReport"]: 
                return default_gid
            # return "UNKNOWN"
            if not row['patient_ref']:
                return f"Orphan-{row['resourceType']}"
            return f"Unknown-{row['patient_ref']}"
        
        df['gid'] = df.apply(get_owner, axis=1)
        # df['final_fp'] = df['gid'] + "|" + df['fingerprint_id']
        # df['final_fp'] = df['resourceType'] + "|" + df['code'] + "|" + df['date_key'] + "|" + df['gid']
        df['final_fp'] = (
        df['gid'].astype(str) + "|" + 
        df['resourceType'].astype(str) + "|" + 
        df['code'].astype(str) + "|" + 
        df['date_key'].astype(str)
        )
        df = df.sort_values(by=['final_fp', 'essence_str'])
        df['occurrence'] = df.groupby('final_fp').cumcount() + 1
        df['final_fp'] = df['final_fp'] + "|seq-" + df['occurrence'].astype(str)
        return df.set_index('final_fp')  

    idx1, idx2 = build_index(raw1), build_index(raw2)
    all_fps = sorted(list(set(idx1.index) | set(idx2.index)))
    changes = []

    for fp in all_fps:
        if "Patient" in fp: continue
        in1, in2 = fp in idx1.index, fp in idx2.index

        if in1 and in2:
            h1, h2 = idx1.loc[[fp], 'payload_hash'].iloc[0], idx2.loc[[fp], 'payload_hash'].iloc[0]
            if h1 != h2:
                e1, e2 = idx1.loc[[fp], 'essence'].iloc[0], idx2.loc[[fp], 'essence'].iloc[0]
                diff = deep_diff(e1, e2)
                if diff: changes.append({"Status": "MODIFIED", "Event": fp, "Detail": diff})
                else: changes.append({"Status": "UNCHANGED", "Event": fp})
            else:
                changes.append({"Status": "UNCHANGED", "Event": fp})
        elif in1:
            changes.append({"Status": "REMOVED", "Event": fp})
        else:
            changes.append({"Status": "ADDED", "Event": fp})

    # Output Final Report
    print("\n" + "="*80 + "\nAUDIT SUMMARY STATS\n" + "="*80)
    report = pd.DataFrame(changes)
    if not report.empty:
        print(report['Status'].value_counts().to_string())
        for status in ["MODIFIED", "REMOVED", "ADDED"]:
            subset = report[report["Status"] == status]
            if not subset.empty:
                sample_event = subset.iloc[0]["Event"]
                print(f"\n================ {status} SAMPLE ================")
                debug_event(sample_event, f1_data, f2_data)
        # for status in ["MODIFIED", "ADDED", "REMOVED"]:
        #     subset = report[report['Status'] == status]
        #     if not subset.empty:
        #         print(f"\n--- {status} ({len(subset)}) ---")
        #         for _, r in subset.iterrows():
        #             print(f" • {r['Event']}")
        #             if status == "MODIFIED": print(f"   Change: {r['Detail']}")
    print("\n--- SAMPLE REMOVED ---")
    print(report[report['Status']=="REMOVED"].head(5))

    print("\n--- SAMPLE ADDED ---")
    print(report[report['Status']=="ADDED"].head(5))

    print("\n--- SAMPLE MODIFIED ---")
    print(report[report['Status']=="MODIFIED"].head(5))

def parse_fp(fp):
    parts = fp.split("|")

    return {
        "gid": parts[0],
        "resourceType": parts[1],
        "code": parts[3] if len(parts) > 3 else None,
        "date": parts[4] if len(parts) > 4 else None
    }
def debug_event(event, f1_data, f2_data):
    parsed = parse_fp(event)

    print("\n🔍 DEBUGGING EVENT:", event)
    print("Parsed:", parsed)

    matches_f1 = find_record(
        f1_data,
        parsed["resourceType"],
        parsed["code"],
        parsed["date"]
    )

    matches_f2 = find_record(
        f2_data,
        parsed["resourceType"],
        parsed["code"],
        parsed["date"]
    )

    print("\n--- FILE 1 MATCH ---")
    print(json.dumps(matches_f1, indent=2) if matches_f1 else "❌ NOT FOUND")

    print("\n--- FILE 2 MATCH ---")
    print(json.dumps(matches_f2, indent=2) if matches_f2 else "❌ NOT FOUND")

# def find_record(data, resource_type, code, date):
#     matches = []

#     for item in data:
#         if item.get("resourceType") != resource_type:
#             continue

#         item_code = (
#             get_val(item, "code.coding.0.code") or
#             get_val(item, "medicationCodeableConcept.coding.0.code") or
#             clean_id(get_val(item, "medicationReference.reference"))
#         )

#         item_date = (
#             get_val(item, "effectiveDateTime") or
#             get_val(item, "authoredOn") or
#             get_val(item, "meta.lastUpdated")
#         )

#         if item_code == code and item_date and date in item_date:
#             matches.append(item)

#     return matches
def find_record(data, resource_type, code, date):
    """
    Finds records in the raw JSON that match the fingerprint components.
    Ensures the logic matches extract_features exactly.
    """
    matches = []

    for item in data:
        # 1. Basic Type Check
        if item.get("resourceType") != resource_type:
            continue

        # 2. Replicate "Code" Extraction Logic from extract_features
        # This must check every path that extract_features checks
        item_internal_id = clean_id(item.get('id'))
        
        if resource_type == "Binary":
            item_code = item_internal_id
        else:
            item_code = (get_val(item, "code.coding.0.code") or 
                        get_val(item, "vaccineCode.coding.0.code") or 
                        get_val(item, "type.0.coding.0.code") or 
                        get_val(item, "type.coding.0.code") or 
                        get_val(item, "category.0.coding.0.code") or 
                        get_val(item, "category.1.coding.0.code") or 
                        get_val(item, "name") or    
                        get_val(item, "class.code") or 
                        get_val(item, "medicationCodeableConcept.coding.0.code") or 
                        clean_id(get_val(item, "medicationReference.reference")) or 
                        "NOCODE")

        # 3. Replicate "Date" Extraction Logic from extract_features
        date_raw = (get_val(item, "effectiveDateTime") or 
                    get_val(item, "performedDateTime") or 
                    get_val(item, "recordedDate") or 
                    get_val(item, "onsetDateTime") or
                    get_val(item, "authoredOn") or 
                    get_val(item, "occurrenceDateTime") or 
                    get_val(item, "period.start") or 
                    get_val(item, "date") or 
                    get_val(item, "meta.lastUpdated") or "NODATE")
        
        # Normalize to the same 16-character string format used in fingerprints
        item_date_key = str(date_raw)[:16].upper()

        # 4. Compare
        # Note: 'code' and 'date' passed into this function come from the fingerprint
        if str(item_code) == str(code) and item_date_key == str(date):
            matches.append(item)

    return matches

if __name__ == "__main__":
    f1_data = json.load(open('elijah.json'))
    f2_data = json.load(open('cigna_synthetic.json'))
    main('elijah.json', 'cigna_synthetic.json')
    # matches_f1 = find_record(f1_data, "Medication", "313782", "2022-08-19T18:39")
    # matches_f2 = find_record(f2_data, "Medication", "313782", "2022-08-19T18:39")

    # print("\n--- FILE 1 MATCH ---")
    # print(json.dumps(matches_f1, indent=2))

    # print("\n--- FILE 2 MATCH ---")
    # print(json.dumps(matches_f2, indent=2))