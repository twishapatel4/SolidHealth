import json
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
import hashlib
import re
from collections import Counter

# --- UTILS ---
def normalize_string(s):
    """Normalizes names and codes for identity matching."""
    if not s or s == "N/A" or s is None: return "na"
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
    Recursively removes platform noise.
    Identifies if the core clinical content of the resource is the same.
    """
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
        
        # Determine the patient this record belongs to
        raw_ref = (item.get('subject', {}).get('reference') or 
                   item.get('patient', {}).get('reference') or 
                   item.get('subject', {}).get('display') or "")
        patient_ref = clean_id(raw_ref)
        
        # 1. SPECIAL CASE: Patient Identity (The Bridge)
        if res_type == "Patient":
            fname = normalize_string(get_val(item, "name.0.family"))
            gname = normalize_string(get_val(item, "name.0.given.0"))
            dob = str(get_val(item, 'birthDate'))
            clinical_id = f"{fname}|{gname}|{dob}"
        else:
            # 2. GENERAL CASE: Clinical Identity (Finding Code/Date)
            code = (get_val(item, "code.coding.0.code") or 
                    get_val(item, "vaccineCode.coding.0.code") or 
                    get_val(item, "type.0.coding.0.code") or 
                    get_val(item, "category.0.coding.0.code") or 
                    get_val(item, "medicationCodeableConcept.coding.0.code") or 
                    clean_id(get_val(item, "medicationReference.reference")) or 
                    normalize_string(get_val(item, "name")) or "no-code")
            
            date_raw = (get_val(item, "effectiveDateTime") or 
                        get_val(item, "performedDateTime") or 
                        get_val(item, "recordedDate") or 
                        get_val(item, "authoredOn") or 
                        get_val(item, "occurrenceDateTime") or 
                        get_val(item, "period.start") or "no-date")
            clinical_id = f"{code}|{str(date_raw)[:10]}"

        # Instance tracking for multiple same-day resources
        instance_tracker[f"{res_type}|{clinical_id}"] += 1
        seq = instance_tracker[f"{res_type}|{clinical_id}"]

        rows.append({
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "clinical_id": clinical_id,
            "fingerprint_id": f"{res_type}|{clinical_id}|{seq}",
            "payload_hash": generate_clinical_hash(item),
            "essence": get_clinical_essence(item)
        })
    return rows, file_stats

# --- STAGE 2: IDENTITY RESOLUTION (The Gatekeeper) ---
def resolve_identity(df_all):
    """
    Determines if File 1 and File 2 contain the same patient.
    Maps the different local IDs to a single Global Identity.
    """
    df_pats = df_all[df_all['resourceType'] == 'Patient'].copy()
    
    # Group by the clinical identity (Name + DOB)
    p1_ids = set(df_pats[df_pats['source'] == 'P1']['clinical_id'])
    p2_ids = set(df_pats[df_pats['source'] == 'P2']['clinical_id'])
    
    common_identities = p1_ids.intersection(p2_ids)
    
    if not common_identities:
        return {}, False

    # Create mapping: Internal Platform ID -> Global Identity String
    mapping = {}
    for _, row in df_pats.iterrows():
        if row['clinical_id'] in common_identities:
            mapping[row['internal_id']] = f"PATIENT-{normalize_string(row['clinical_id'])}"
        else:
            mapping[row['internal_id']] = f"UNRESOLVED-{row['internal_id']}"
            
    return mapping, True

# --- STAGE 3: MAIN EXECUTION ---
def main(file1, file2):
    print(f"Step 1: Comparing Patient identities between {file1} and {file2}...")
    
    raw1, stats1 = extract_features(json.load(open(file1)), "P1")
    raw2, stats2 = extract_features(json.load(open(file2)), "P2")
    df_all = pd.concat([pd.DataFrame(raw1), pd.DataFrame(raw2)])

    id_map, same_patient = resolve_identity(df_all)

    if not same_patient:
        print("❌ ABORT: These files appear to belong to different patients. Comparison skipped.")
        return

    print("✅ Match Confirmed: Both files belong to the same resolved clinical identity.")

    # Identify the primary patient ID for the audit
    default_gid = list(id_map.values())[0]

    def build_index(rows):
        df = pd.DataFrame(rows)
        def get_owner(row):
            gid = id_map.get(row['patient_ref'])
            if gid: return gid
            # Fallback for ownerless infrastructure like Organization/Location
            if row['resourceType'] in ["Location", "Organization", "Practitioner", "Medication"]: 
                return default_gid
            return "UNKNOWN"
        
        df['gid'] = df.apply(get_owner, axis=1)
        df['final_fp'] = df['gid'] + "|" + df['fingerprint_id']
        return df.set_index('final_fp')

    idx1, idx2 = build_index(raw1), build_index(raw2)
    all_fps = sorted(list(set(idx1.index) | set(idx2.index)))
    changes = []

    for fp in all_fps:
        if "Patient" in fp: continue # Patient is the anchor, skip in clinical audit
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

    # Display Report
    print("\n" + "="*80 + "\nCROSS-PLATFORM CLINICAL AUDIT SUMMARY\n" + "="*80)
    report = pd.DataFrame(changes)
    if not report.empty:
        print(report['Status'].value_counts().to_string())
        for status in ["MODIFIED", "ADDED", "REMOVED"]:
            subset = report[report['Status'] == status]
            if not subset.empty:
                print(f"\n--- {status} ({len(subset)}) ---")
                for _, r in subset.iterrows():
                    print(f" • {r['Event']}")
                    if status == "MODIFIED": print(f"   Change: {r['Detail']}")
    
    # Final check
    target = stats1.total() - stats1['Patient']
    actual = len(report[report['Status'].isin(['UNCHANGED', 'MODIFIED', 'REMOVED'])])
    print(f"\nVerification: Audit accounted for {actual} clinical records from File 1 (Target: {target})")

if __name__ == "__main__":
    main('elijah.json', 'new_data.json')