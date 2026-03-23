import json
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
import hashlib
import re
from collections import Counter

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

# --- CLINICAL HASHING LOGIC ---
def get_clinical_essence(obj):
    """
    Recursively removes technical noise and platform identifiers.
    Normalizes dates and numbers for stable hashing.
    """
    # Noise keys that are platform-dependent or non-readable
    noise = {
        'id', 'meta', 'text', 'reference', 'lastUpdated', 'versionId', 
        'url', 'system', 'fullUrl', 'identifier', 'data', 'attachment'
    }
    
    if isinstance(obj, dict):
        cleaned = {k: get_clinical_essence(v) for k, v in obj.items() if k not in noise}
        return {k: v for k, v in cleaned.items() if v not in [None, "", [], {}]}
    elif isinstance(obj, list):
        return sorted([get_clinical_essence(x) for x in obj], key=lambda x: str(x))
    elif isinstance(obj, (int, float)):
        return float(obj)
    elif isinstance(obj, str):
        # Truncate timestamps to Date only for Identity logic
        if re.match(r'\d{4}-\d{2}-\d{2}t', obj.lower()):
            return obj[:10]
        return obj.strip().lower()
    return obj

def generate_clinical_hash(obj):
    """SHA256 of the clinical essence."""
    essence = get_clinical_essence(obj)
    serialized = json.dumps(essence, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized).hexdigest()

def deep_diff(d1, d2, path=""):
    """Finds readable clinical differences between two objects."""
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        return f"{d1} ➔ {d2}"
    
    diffs = []
    keys = set(d1.keys()) | set(d2.keys())
    for k in keys:
        # Skip technical data fields in the final printout for readability
        if k in ['data', 'attachment', 'content']: continue
        
        v1, v2 = d1.get(k), d2.get(k)
        if v1 != v2:
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub = deep_diff(v1, v2, f"{path}{k}.")
                if sub: diffs.append(sub)
            else:
                diffs.append(f"{path}{k}: {v1} ➔ {v2}")
    return ", ".join(diffs)

# --- STAGE 1: FEATURE EXTRACTION ---
def extract_features(data, source_name):
    rows = []
    file_stats = Counter()
    instance_tracker = Counter()

    for item in data:
        res_type = item.get('resourceType')
        if res_type == "Provenance": continue
        
        file_stats[res_type] += 1
        internal_id = clean_id(item.get('id'))
        raw_ref = (item.get('subject', {}).get('reference') or 
                   item.get('patient', {}).get('reference') or 
                   item.get('subject', {}).get('display') or "")
        patient_ref = clean_id(raw_ref)
        
        # 1. Improved Identity Logic (Finding Code/Date)
        # code = (get_val(item, "code.coding.0.code") or 
        #         get_val(item, "vaccineCode.coding.0.code") or 
        #         get_val(item, "type.0.coding.0.code") or 
        #         get_val(item, "class.code") or 
        #         get_val(item, "medicationCodeableConcept.coding.0.code") or "NOCODE")
        code = (get_val(item, "code.coding.0.code") or 
                get_val(item, "vaccineCode.coding.0.code") or 
                get_val(item, "type.0.coding.0.code") or 
                get_val(item, "type.coding.0.code") or 
                get_val(item, "class.code") or 
                get_val(item, "medicationCodeableConcept.coding.0.code") or 
                clean_id(get_val(item, "medicationReference.reference")) or # Specific for your Meds
                get_val(item, "category.0.coding.0.code") or 
                "NOCODE")
        
        # date_raw = (get_val(item, "effectiveDateTime") or 
        #             get_val(item, "performedDateTime") or 
        #             get_val(item, "recordedDate") or 
        #             get_val(item, "onsetDateTime") or
        #             get_val(item, "authoredOn") or 
        #             get_val(item, "period.start") or "NODATE")
        
        date_raw = (get_val(item, "effectiveDateTime") or 
                    get_val(item, "recordedDate") or 
                    get_val(item, "performedDateTime") or 
                    get_val(item, "authoredOn") or        # Specific for Meds
                    get_val(item, "occurrenceDateTime") or # Specific for Immunizations
                    get_val(item, "period.start") or 
                    get_val(item, "date") or 
                    get_val(item, "meta.lastUpdated") or   # Technical fallback
                    "NODATE")
        # date_key = date_raw[:10] if date_raw != "NODATE" else "NODATE"
        date_key = date_raw[:16] if date_raw != "NODATE" else "NODATE"

        # Create unique instance fingerprint
        clinical_id = f"{code}|{date_key}"
        instance_tracker[f"{res_type}|{clinical_id}"] += 1
        seq = instance_tracker[f"{res_type}|{clinical_id}"]

        rows.append({
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "fingerprint_id": f"{res_type}|{clinical_id}|{seq}",
            "payload_hash": generate_clinical_hash(item),
            "essence": get_clinical_essence(item)
        })
    return rows, file_stats

# --- STAGE 2: IDENTITY RESOLUTION ---
def get_patient_matches(df_patients):
    if len(df_patients) < 2: return pd.DataFrame()
    settings = {"link_type": "dedupe_only", "unique_id_column_name": "linkage_id",
                "comparisons": [{"output_column_name": "res_type", "comparison_levels": [
                    {"sql_condition": "resourceType_l = resourceType_r", "m_probability": 0.99, "u_probability": 0.01},
                    {"sql_condition": "ELSE", "m_probability": 0.01, "u_probability": 0.99}]}]}
    return Linker(df_patients, settings, DuckDBAPI()).inference.predict(threshold_match_probability=0.9).as_pandas_dataframe()

def create_global_id_map(df_patients, df_matches):
    G = nx.Graph()
    l_to_i = dict(zip(df_patients['linkage_id'], df_patients['internal_id']))
    for lid in df_patients['linkage_id']: G.add_node(lid)
    if not df_matches.empty:
        for _, row in df_matches.iterrows(): G.add_edge(row['linkage_id_l'], row['linkage_id_r'])
    mapping = {}
    for cluster in nx.connected_components(G):
        unique_global_id = "PATIENT-ELIJAH-FISHER"
        for lid in cluster: mapping[l_to_i[lid]] = unique_global_id
    return mapping

# --- MAIN EXECUTION ---
def main(file1, file2):
    raw1, stats1 = extract_features(json.load(open(file1)), "P1")
    raw2, stats2 = extract_features(json.load(open(file2)), "P2")

    df_all = pd.concat([pd.DataFrame(raw1), pd.DataFrame(raw2)])
    # id_map = create_global_id_map(df_all[df_all['resourceType']=='Patient'], get_patient_matches(df_all[df_all['resourceType']=='Patient']))
    patient_matches = get_patient_matches(df_all[df_all['resourceType']=='Patient'])
    id_map = create_global_id_map(df_all[df_all['resourceType']=='Patient'], patient_matches)
    
    # NEW: Get the first resolved Patient ID to use as a fallback for infrastructure
    default_gid = list(id_map.values())[0] if id_map else "GLOBAL-SYSTEM"
    # def build_index(rows):
    #     df = pd.DataFrame(rows)
    #     df['gid'] = df['patient_ref'].apply(lambda x: id_map.get(x, "UNKNOWN"))
    #     df['final_fp'] = df['gid'] + "|" + df['fingerprint_id']
    #     return df.set_index('final_fp')
    def build_index(rows):
        df = pd.DataFrame(rows)
        
        def get_owner(row):
            # 1. Try to find the real link
            gid = id_map.get(row['patient_ref'])
            if gid: return gid
            
            # 2. Fallback for Infrastructure resources
            infra_types = ["Location", "Organization", "Practitioner", "Medication"]
            if row['resourceType'] in infra_types:
                return default_gid
            
            return "UNKNOWN"

        df['gid'] = df.apply(get_owner, axis=1)
        df['final_fp'] = df['gid'] + "|" + df['fingerprint_id']
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
                if diff:
                    changes.append({"Status": "MODIFIED", "Event": fp, "Detail": diff})
                else:
                    changes.append({"Status": "UNCHANGED", "Event": fp})
            else:
                changes.append({"Status": "UNCHANGED", "Event": fp})
        elif in1:
            changes.append({"Status": "REMOVED", "Event": fp})
        else:
            changes.append({"Status": "ADDED", "Event": fp})

    # --- PRINT REPORT ---
    print("\n" + "="*80 + "\nMETADATA: PHYSICAL RESOURCE COUNTS\n" + "="*80)
    all_types = sorted(list(set(stats1.keys()) | set(stats2.keys())))
    for rt in all_types:
        print(f"{rt:<25} | File 1: {stats1[rt]:<4} | File 2: {stats2[rt]:<4}")

    report = pd.DataFrame(changes)
    print("\n" + "="*80 + "\nAUDIT SUMMARY STATS\n" + "="*80)
    if not report.empty:
        print(report['Status'].value_counts().to_string())
        for status in ["MODIFIED", "ADDED", "REMOVED"]:
            subset = report[report['Status'] == status]
            if not subset.empty:
                print(f"\n--- {status} ({len(subset)}) ---")
                for _, r in subset.iterrows():
                    print(f" • {r['Event']}")
                    if status == "MODIFIED": print(f"   Change: {r['Detail']}")

if __name__ == "__main__":
    main('elijah.json', 'cigna_synthetic.json')