import json
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
import hashlib
import uuid
import re
from collections import Counter
import base64

# --- UTILS ---
def hash_binary_data(data):
    if not data: return None
    try:
        decoded = base64.b64decode(data)
        return hashlib.sha256(decoded).hexdigest()
    except: return None

def normalize_string(s):
    if not s or s == "N/A": return "na"
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def clean_id(raw_id):
    if raw_id is None or pd.isna(raw_id): return "MISSING"
    return str(raw_id).split('/')[-1].strip()

def get_val(obj, path, default=None):
    components = path.split('.')
    for part in components:
        if isinstance(obj, dict): obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit():
            try: obj = obj[int(part)]
            except: return default
        else: return default
    return obj if obj is not None else default

def normalize_system(system):
    if not system: return "unknown"
    s = str(system).lower()
    if any(k in s for k in ["aetna", "cigna", "humana", "payer"]):
        return "payer_system"
    if "mrn" in s: return "mrn"
    if "ssn" in s: return "ssn"
    if "loinc" in s: return "loinc"
    if "snomed" in s: return "snomed"
    # Strip protocol
    return re.sub(r'^https?://', '', s).rstrip('/')

def normalize_identifier(identifier):
    if not identifier: return None
    if isinstance(identifier, list):
        return sorted([normalize_identifier(i) for i in identifier if i], key=lambda x: str(x))
    
    if not isinstance(identifier, dict): return None

    # We keep the 'system' (e.g., cigna vs aetna) and the 'type'
    # But we ignore the 'value' if it looks like a system GUID
    val = str(identifier.get("value", ""))
    # If the value is a UUID/GUID, it's noise. Mask it.
    if re.match(r'^[a-f0-9\-]{32,36}$', val.lower()):
        val = "PLATFORM_GUID_MASK"

    return {
        "type": normalize_string(get_val(identifier, "type.coding.0.code") or "unknown"),
        "system": normalize_system(identifier.get("system")),
        "value": val 
    }

# --- CLINICAL HASHING LOGIC ---
def get_clinical_essence(obj):
    # Expanded noise list
    noise = {
        'id', 'meta', 'text', 'reference', 'lastUpdated', 'versionId', 
        'url', 'fullUrl', 'extension' # Extensions often carry platform metadata
    }

    if isinstance(obj, dict):
        # 🔵 HANDLE REFERENCES (The biggest source of MODIFIED false positives)
        # If it's a reference, we only care about the ResourceType, not the ID
        if "reference" in obj and isinstance(obj["reference"], str):
            ref_parts = obj["reference"].split('/')
            return {"reference_type": ref_parts[0]} # e.g., "Practitioner" instead of "Practitioner/123"

        # 🔵 HANDLE RESOURCE-SPECIFIC LOGIC
        if obj.get("resourceType") == "Binary":
            return {"res": "Binary", "hash": hash_binary_data(obj.get("data"))}

        cleaned = {}
        for k, v in obj.items():
            if k in noise: continue
            if k == "identifier":
                cleaned[k] = normalize_identifier(v)
            elif k == "system":
                cleaned[k] = normalize_system(v)
            else:
                cleaned[k] = get_clinical_essence(v)

        # Return dict sorted by key to ensure consistent hashing
        return {k: cleaned[k] for k in sorted(cleaned.keys()) if cleaned[k] not in [None, "", [], {}]}

    elif isinstance(obj, list):
        # Sort all lists by their string representation
        # This prevents MODIFIED flags caused by item order changes
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
    if not isinstance(d1, dict) or not isinstance(d2, dict): return f"{d1} ➔ {d2}"
    diffs = []
    for k in (set(d1.keys()) | set(d2.keys())):
        v1, v2 = d1.get(k), d2.get(k)
        if v1 != v2:
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub = deep_diff(v1, v2, f"{path}{k}.")
                if sub: diffs.append(sub)
            else: diffs.append(f"{path}{k}: {v1} ➔ {v2}")
    return ", ".join(diffs)

def get_fhir_date(item, res_type):
    """
    Extracts the most clinically relevant date from a FHIR resource.
    Returns a normalized minute-level string or 'NODATE'.
    """

    def format_date(val):
        if not val:
            return None
        return str(val)[:16].upper()

    # 🔵 1. Common high-priority fields (fast path)
    common_fields = [
        "effectiveDateTime",
        "performedDateTime",
        "recordedDate",
        "issued",
        "authoredOn",
        "date"
    ]
    
    for field in common_fields:
        val = get_val(item, field)
        if val:
            return format_date(val)

    # 🔵 2. Period-based fields (VERY common in FHIR)
    period_fields = [
        "effectivePeriod.start",
        "performedPeriod.start",
        "period.start"
    ]

    for field in period_fields:
        val = get_val(item, field)
        if val:
            return format_date(val)

    # 🔵 3. Resource-specific logic

    if res_type == "Condition":
        val = (
            get_val(item, "onsetDateTime") or
            get_val(item, "period.start") or
            get_val(item, "abatementDateTime") or
            get_val(item, "abatementPeriod.start")
        )
        if val:
            return format_date(val)
    elif res_type == "CarePlan":
        val = get_val(item, "period.start")
        if val:
            return format_date(val)

    elif res_type == "Observation":
        val = (
            get_val(item, "effectiveDateTime") or
            get_val(item, "effectivePeriod.start") or
            get_val(item, "issued")
        )
        if val:
            return format_date(val)

    elif res_type == "MedicationRequest":
        val = get_val(item, "authoredOn")
        if val:
            return format_date(val)

    elif res_type == "Procedure":
        val = (
            get_val(item, "performedDateTime") or
            get_val(item, "performedPeriod.start")
        )
        if val:
            return format_date(val)

    elif res_type == "Encounter":
        val = get_val(item, "period.start")
        if val:
            return format_date(val)

    elif res_type == "AllergyIntolerance":
        val = get_val(item, "recordedDate")
        if val:
            return format_date(val)

    elif res_type == "Immunization":
        val = get_val(item, "occurrenceDateTime")
        if val:
            return format_date(val)

    elif res_type == "DiagnosticReport":
        val = (
            get_val(item, "effectiveDateTime") or
            get_val(item, "issued")
        )
        if val:
            return format_date(val)

    elif res_type == "DocumentReference":
        val = (
            get_val(item, "date") or
            get_val(item, "context.period.start")
        )
        if val:
            return format_date(val)

    # 🔴 4. Final fallback
    return "NODATE"
# --- STAGE 1: FEATURE EXTRACTION ---
def extract_features(data, source_name):
    temp_list = []
    file_stats = Counter()
    for item in data:
        res_type = item.get('resourceType')
        if res_type in ["Provenance", None]: continue
        file_stats[res_type] += 1
        internal_id = clean_id(item.get('id'))
        patient_ref = clean_id(get_val(item, "subject.reference") or get_val(item, "patient.reference") or "")
        
        # Identity Logic
        if res_type == "Binary": code = internal_id
        else:
            code = (get_val(item, "code.coding.0.code") or get_val(item, "vaccineCode.coding.0.code") or 
                    get_val(item, "type.coding.0.code") or get_val(item, "category.0.coding.0.code") or "NOCODE")
        
        # date_raw = (get_val(item, "effectiveDateTime") or get_val(item, "performedDateTime") or 
        #             get_val(item, "recordedDate") or get_val(item, "date") or "NODATE")
        # date_key = str(date_raw)[:16].upper()
        date_key = get_fhir_date(item, res_type)
        
        # Demographics for Splink (Patient only)
        row = {
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "code": str(code),
            "date_key": date_key,
            "payload_hash": generate_clinical_hash(item),
            "essence": get_clinical_essence(item)
        }
        if res_type == "Patient":
            row["first_name"] = normalize_string(get_val(item, "name.0.given.0"))
            row["last_name"] = normalize_string(get_val(item, "name.0.family"))
            row["dob"] = get_val(item, "birthDate")
        temp_list.append(row)
    return temp_list, file_stats

# --- IDENTITY RESOLUTION (Matching Patient A to Patient B) ---
def get_patient_matches(df_patients):
    if len(df_patients) < 2: return pd.DataFrame()
    # Improved Settings: Match on Name + DOB, not just ID!
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "linkage_id",
        "comparisons": [
            {"output_column_name": "first_name", "comparison_levels": [{"sql_condition": "first_name_l = first_name_r", "m_probability": 0.9}, {"sql_condition": "ELSE", "m_probability": 0.1}]},
            {"output_column_name": "last_name", "comparison_levels": [{"sql_condition": "last_name_l = last_name_r", "m_probability": 0.9}, {"sql_condition": "ELSE", "m_probability": 0.1}]},
            {"output_column_name": "dob", "comparison_levels": [{"sql_condition": "dob_l = dob_r", "m_probability": 0.9}, {"sql_condition": "ELSE", "m_probability": 0.1}]},
        ]
    }
    return Linker(df_patients, settings, DuckDBAPI()).inference.predict(threshold_match_probability=0.8).as_pandas_dataframe()

def create_global_id_map(df_patients, df_matches):
    G = nx.Graph()
    l_to_i = dict(zip(df_patients['linkage_id'], df_patients['internal_id']))
    for lid in df_patients['linkage_id']: G.add_node(lid)
    if not df_matches.empty:
        for _, row in df_matches.iterrows(): G.add_edge(row['linkage_id_l'], row['linkage_id_r'])
    mapping = {}
    for cluster in nx.connected_components(G):
        unique_global_id = f"GID-{hashlib.md5(str(sorted(list(cluster))).encode()).hexdigest()[:8].upper()}"
        for lid in cluster: mapping[l_to_i[lid]] = unique_global_id
    return mapping

# --- MAIN ---
def main(file1, file2):
    raw1_list, _ = extract_features(json.load(open(file1)), "P1")
    raw2_list, _ = extract_features(json.load(open(file2)), "P2")
    df_all = pd.concat([pd.DataFrame(raw1_list), pd.DataFrame(raw2_list)])
    
    # Link Patients
    id_map = create_global_id_map(df_all[df_all['resourceType']=='Patient'], get_patient_matches(df_all[df_all['resourceType']=='Patient']))
    default_gid = list(id_map.values())[0] if id_map else "GLOBAL-SYSTEM"

    def build_index(rows):
        df = pd.DataFrame(rows)
        df['gid'] = df['patient_ref'].map(id_map).fillna(df['internal_id'].map(id_map)).fillna(default_gid)
        # Fingerprint: GID | Type | Code | Date
        df['fp_base'] = df['gid'] + "|" + df['resourceType'] + "|" + df['code'] + "|" + df['date_key']
        df = df.sort_values(by=['fp_base', 'payload_hash'])
        df['seq'] = df.groupby('fp_base').cumcount() + 1
        df['final_fp'] = df['fp_base'] + "|seq-" + df['seq'].astype(str)
        return df.set_index('final_fp')

    idx1, idx2 = build_index(raw1_list), build_index(raw2_list)
    all_fps = sorted(list(set(idx1.index) | set(idx2.index)))
    changes = []

    for fp in all_fps:
        if "|Patient|" in fp: continue
        in1, in2 = fp in idx1.index, fp in idx2.index
        if in1 and in2:
            row1, row2 = idx1.loc[fp], idx2.loc[fp]
            if row1['payload_hash'] != row2['payload_hash']:
                diff = deep_diff(row1['essence'], row2['essence'])
                changes.append({"Status": "MODIFIED", "Event": fp, "Detail": diff})
            else: changes.append({"Status": "UNCHANGED", "Event": fp})
        elif in1: changes.append({"Status": "REMOVED", "Event": fp})
        else: changes.append({"Status": "ADDED", "Event": fp})

    report = pd.DataFrame(changes)
    print("\n" + "="*40 + "\nAUDIT SUMMARY\n" + "="*40)
    print(report['Status'].value_counts() if not report.empty else "No records found")
    
    for status in ["MODIFIED", "REMOVED", "ADDED"]:
        subset = report[report["Status"] == status]
        if not subset.empty:
            print(f"\n--- {status} SAMPLE ---")
            row = subset.sample(1).iloc[0]
            debug_event(row["Event"], idx1, idx2)

def parse_fp(fp):
    parts = fp.split("|")
    # Correct mapping: 0:GID, 1:Type, 2:Code, 3:Date, 4:Seq
    return {"gid": parts[0], "resourceType": parts[1], "code": parts[2], "date": parts[3]}

def debug_event(event, idx1, idx2):
    print(f"Event: {event}")
    
    in1 = event in idx1.index
    in2 = event in idx2.index

    print(f"File 1 Exact Match: {'✅ FOUND' if in1 else '❌ NOT FOUND'}")
    print(f"File 2 Exact Match: {'✅ FOUND' if in2 else '❌ NOT FOUND'}")

    if in1:
        print(f"File1 Payload Hash: {idx1.loc[event]['payload_hash']}")
    if in2:
        print(f"File2 Payload Hash: {idx2.loc[event]['payload_hash']}")

if __name__ == "__main__":
    main('elijah.json', 'cigna_synthetic.json')