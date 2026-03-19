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

# --- CLINICAL HASHING ---
def get_clinical_essence(obj):
    """Recursively removes technical noise."""
    noise = {
        'id', 'meta', 'text', 'reference', 'lastUpdated', 'versionId', 
        'url', 'system', 'fullUrl', 'identifier' 
    }
    
    if isinstance(obj, dict):
        cleaned = {k: get_clinical_essence(v) for k, v in obj.items() if k not in noise}
        return {k: v for k, v in cleaned.items() if v not in [None, "", [], {}]}
    elif isinstance(obj, list):
        return sorted([get_clinical_essence(x) for x in obj], key=lambda x: str(x))
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
    differences = []
    keys = set(d1.keys()) | set(d2.keys())
    for k in keys:
        v1, v2 = d1.get(k), d2.get(k)
        if v1 != v2:
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub = deep_diff(v1, v2, f"{path}{k}.")
                if sub: differences.append(sub)
            else:
                differences.append(f"{path}{k}: {v1} ➔ {v2}")
    return ", ".join(differences)

# --- FEATURE EXTRACTION ---
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
        
        # Identity Logic: Find Code
        code = (get_val(item, "code.coding.0.code") or 
                get_val(item, "vaccineCode.coding.0.code") or 
                get_val(item, "type.0.coding.0.code") or 
                get_val(item, "type.coding.0.code") or 
                get_val(item, "class.code") or 
                get_val(item, "medicationCodeableConcept.coding.0.code") or "NOCODE")
        
        # Identity Logic: Find Date (Added performedDateTime and Period)
        date_raw = (get_val(item, "effectiveDateTime") or 
                    get_val(item, "performedDateTime") or 
                    get_val(item, "recordedDate") or 
                    get_val(item, "onsetDateTime") or
                    get_val(item, "authoredOn") or 
                    get_val(item, "occurrenceDateTime") or 
                    get_val(item, "period.start") or 
                    get_val(item, "effectivePeriod.start") or "NODATE")
        
        # We use the first 10 chars (Date) for the identity key to allow 
        # matching between systems with slight clock skew, but different days 
        # will now correctly result in different fingerprints.
        date_key = date_raw[:10] if date_raw != "NODATE" else "NODATE"

        clinical_key = f"{code}|{date_key}"
        instance_tracker[f"{res_type}|{clinical_key}"] += 1
        seq = instance_tracker[f"{res_type}|{clinical_key}"]

        rows.append({
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "fingerprint_id": f"{res_type}|{clinical_key}|{seq}",
            "payload": generate_clinical_hash(item),
            "essence_data": get_clinical_essence(item)
        })
    return rows, file_stats

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
        unique_global_id = "PATIENT-ELIJAH-FISHER"
        for lid in cluster: mapping[l_to_i[lid]] = unique_global_id
    return mapping

# --- MAIN ---
def main(file1, file2):
    raw1, stats1 = extract_features(json.load(open(file1)), "P1")
    raw2, stats2 = extract_features(json.load(open(file2)), "P2")

    df_all = pd.concat([pd.DataFrame(raw1), pd.DataFrame(raw2)])
    id_map = create_global_id_map(df_all[df_all['resourceType']=='Patient'], get_patient_matches(df_all[df_all['resourceType']=='Patient']))

    def build_index(rows):
        df = pd.DataFrame(rows)
        df['gid'] = df['patient_ref'].apply(lambda x: id_map.get(x, "UNKNOWN"))
        df['final_fp'] = df['gid'] + "|" + df['fingerprint_id']
        return df.set_index('final_fp')

    idx1, idx2 = build_index(raw1), build_index(raw2)
    all_fps = sorted(list(set(idx1.index) | set(idx2.index)))
    changes = []

    for fp in all_fps:
        if "Patient" in fp: continue
        in1, in2 = fp in idx1.index, fp in idx2.index

        if in1 and in2:
            h1, h2 = idx1.loc[[fp], 'payload'].iloc[0], idx2.loc[[fp], 'payload'].iloc[0]
            if h1 != h2:
                e1, e2 = idx1.loc[[fp], 'essence_data'].iloc[0], idx2.loc[[fp], 'essence_data'].iloc[0]
                diff = deep_diff(e1, e2)
                changes.append({"Status": "MODIFIED", "Event": fp, "Detail": diff})
            else:
                changes.append({"Status": "UNCHANGED", "Event": fp})
        elif in1:
            changes.append({"Status": "REMOVED", "Event": fp})
        else:
            changes.append({"Status": "ADDED", "Event": fp})

    print("\n" + "="*80 + "\nMETADATA: PHYSICAL COUNTS\n" + "="*80)
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