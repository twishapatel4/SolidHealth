import json, hashlib, re, base64, pandas as pd, networkx as nx
from splink import Linker, DuckDBAPI
from datetime import datetime

# --- [CONFIG & REGISTRY] ---
# Expanded to look deeper into arrays and alternate fields
RESOURCE_MAP = {
    "Observation":        (["code.coding.0.code", "code.coding.1.code", "code.text"], ["effectiveDateTime", "issued"]),
    "MedicationRequest":  (["medicationCodeableConcept.coding.0.code", "medicationCodeableConcept.text", "medicationReference.reference"], ["authoredOn"]),
    "Condition":          (["code.coding.0.code", "code.coding.1.code", "code.text"], ["onsetDateTime", "recordedDate"]),
    "Procedure":          (["code.coding.0.code", "code.text"], ["performedDateTime", "performedPeriod.start"]),
    "Immunization":       (["vaccineCode.coding.0.code", "code.text"], ["occurrenceDateTime"]),
    "Encounter":          (["type.0.coding.0.code", "class.code"], ["period.start"]),
    "Organization":       (["identifier.0.value", "name"], ["STATIC"]),
    "Location":           (["name", "address.city"], ["STATIC"]),
    "Medication":         (["code.coding.0.code", "ingredient.0.itemCodeableConcept.coding.0.code"], ["STATIC"]),
    "Binary":             (["id"], ["STATIC"]),
    "Claim":              (["identifier.0.value", "type.coding.0.code"], ["billablePeriod.start"]),
    "ExplanationOfBenefit": (["identifier.0.value", "type.coding.0.code"], ["created"]),
}
DEFAULT_PATHS = (["code.coding.0.code", "identifier.0.value"], ["date", "meta.lastUpdated"])

# --- [UTILITIES] ---
def get_val(obj, path, default=None):
    for part in path.split('.'):
        if isinstance(obj, dict): obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit():
            try: obj = obj[int(part)]
            except: return default
        else: return default
    return obj if obj is not None else default

def normalize(s):
    if not s: return "na"
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def clean_ref(ref):
    """Removes platform prefixes from references (e.g., Medication/123 -> 123)."""
    if not ref: return "none"
    return str(ref).split('/')[-1]

def normalize_date(date_str):
    """Converts various FHIR date formats to a stable YYYY-MM-DD minute string."""
    if not date_str or date_str == "STATIC": return date_str
    try:
        # Strip timezone offsets for comparison stability
        clean_ts = re.sub(r'([+-]\d{2}:\d{2}|Z)$', '', str(date_str))
        dt = datetime.fromisoformat(clean_ts[:19])
        return dt.strftime("%Y-%m-%dT%H:%M")
    except:
        return str(date_str)[:10] # Fallback to Day level if parsing fails

# --- [DIFFING & DEBUGGING] ---
def deep_diff(d1, d2, path=""):
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        return f"{d1} ➔ {d2}"
    diffs = []
    for k in (set(d1.keys()) | set(d2.keys())):
        v1, v2 = d1.get(k), d2.get(k)
        if v1 != v2:
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub = deep_diff(v1, v2, f"{path}{k}.")
                if sub: diffs.append(sub)
            else:
                diffs.append(f"{path}{k}: {v1} ➔ {v2}")
    return ", ".join(diffs)

def debug_event(event, idx1, idx2):
    print(f"\n🔍 DEBUGGING EVENT: {event}")
    in1, in2 = event in idx1.index, event in idx2.index
    print(f"  File 1: {'✅ FOUND' if in1 else '❌ MISSING'}")
    print(f"  File 2: {'✅ FOUND' if in2 else '❌ MISSING'}")
    if in1 and in2:
        r1, r2 = idx1.loc[event], idx2.loc[event]
        if r1["payload_hash"] == r2["payload_hash"]: print("  Result: UNCHANGED")
        else:
            print("  Result: MODIFIED")
            print(f"  Diff: {deep_diff(r1['essence'], r2['essence'])}")

# --- [ESSENCE & EXTRACTION] ---
def get_clinical_essence(obj):
    noise = {'id', 'meta', 'text', 'reference', 'lastUpdated', 'versionId', 'url', 'extension', 'fullUrl'}
    if isinstance(obj, dict):
        if "reference" in obj: return {"ref_type": clean_ref(obj["reference"].split('/')[0])}
        if obj.get("resourceType") == "Binary":
            return {"res": "Binary", "h": hashlib.sha256(base64.b64decode(obj.get("data", "")) or b"").hexdigest()[:10]}
        cleaned = {k: get_clinical_essence(v) for k, v in obj.items() if k not in noise}
        return {k: cleaned[k] for k in sorted(cleaned.keys()) if cleaned[k] not in [None, "", [], {}]}
    if isinstance(obj, list):
        return sorted([get_clinical_essence(x) for x in obj if x], key=lambda x: str(x))
    return str(obj).strip().lower() if isinstance(obj, str) else obj

def extract_resource_data(item):
    res_type = item.get("resourceType")
    if res_type in ["Provenance", None]: return None
    
    code_paths, date_paths = RESOURCE_MAP.get(res_type, DEFAULT_PATHS)
    
    # Logic: Find the first path that returns a non-null value
    identity_code = next((get_val(item, p) for p in code_paths if get_val(item, p)), "NOCODE")
    date_val = next((get_val(item, p) for p in date_paths if get_val(item, p)), "NODATE")
    
    # Clean code: if it's a reference, strip the ID
    if "/" in str(identity_code): identity_code = clean_ref(identity_code)

    essence = get_clinical_essence(item)
    return {
        "internal_id": clean_ref(item.get("id")),
        "res_type": res_type,
        "patient_ref": clean_ref(get_val(item, "subject.reference") or get_val(item, "patient.reference")),
        "code": normalize(identity_code),
        "date_key": normalize_date(date_val),
        "essence": essence,
        "payload_hash": hashlib.sha256(json.dumps(essence, sort_keys=True).encode()).hexdigest(),
        "first_name": normalize(get_val(item, "name.0.given.0")),
        "last_name": normalize(get_val(item, "name.0.family")),
        "dob": get_val(item, "birthDate"),
    }

# --- [IDENTITY RESOLUTION] ---
def get_gid_map(df):
    patients = df[df["res_type"] == "Patient"].copy()
    if patients.empty: return {}
    patients["unique_id"] = range(len(patients))
    settings = {
        "link_type": "dedupe_only", "unique_id_column_name": "unique_id",
        "comparisons": [
            {"output_column_name": "dob", "comparison_levels": [{"sql_condition": "dob_l = dob_r", "m_probability": 0.95}, {"sql_condition": "ELSE", "m_probability": 0.05}]},
            {"output_column_name": "last_name", "comparison_levels": [{"sql_condition": "last_name_l = last_name_r", "m_probability": 0.9}, {"sql_condition": "ELSE", "m_probability": 0.1}]},
        ]
    }
    matches = Linker(patients, settings, DuckDBAPI()).inference.predict(threshold_match_probability=0.7).as_pandas_dataframe()
    G = nx.Graph()
    G.add_nodes_from(patients["internal_id"])
    for _, r in matches.iterrows():
        G.add_edge(patients.iloc[int(r["unique_id_l"])]["internal_id"], patients.iloc[int(r["unique_id_r"])]["internal_id"])
    
    mapping = {}
    for cluster in nx.connected_components(G):
        gid = f"GID-{hashlib.md5(str(sorted(cluster)).encode()).hexdigest()[:6].upper()}"
        for node in cluster: mapping[node] = gid
    return mapping

# --- [AUDIT ENGINE] ---
def run_audit(file1, file2):
    data1 = [extract_resource_data(i) for i in json.load(open(file1)) if extract_resource_data(i)]
    data2 = [extract_resource_data(i) for i in json.load(open(file2)) if extract_resource_data(i)]
    
    df_full = pd.concat([pd.DataFrame(data1), pd.DataFrame(data2)])
    gid_map = get_gid_map(df_full)
    
    def process_fingerprints(data_list):
        if not data_list: return pd.DataFrame()
        temp_df = pd.DataFrame(data_list)
        temp_df["gid"] = temp_df["patient_ref"].map(gid_map).fillna(temp_df["internal_id"].map(gid_map)).fillna("SYSTEM")
        temp_df["fp_base"] = temp_df["gid"] + "|" + temp_df["res_type"] + "|" + temp_df["code"] + "|" + temp_df["date_key"]
        temp_df = temp_df.sort_values(["fp_base", "payload_hash"])
        temp_df["seq"] = temp_df.groupby("fp_base").cumcount() + 1
        temp_df["final_fp"] = temp_df["fp_base"] + "|seq-" + temp_df["seq"].astype(str)
        return temp_df.set_index("final_fp")

    idx1, idx2 = process_fingerprints(data1), process_fingerprints(data2)
    
    results = []
    all_fps = sorted(set(idx1.index) | set(idx2.index))
    for fp in all_fps:
        if "|Patient|" in fp: continue
        in1, in2 = fp in idx1.index, fp in idx2.index
        if in1 and in2:
            r1, r2 = idx1.loc[fp], idx2.loc[fp]
            status = "UNCHANGED" if r1["payload_hash"] == r2["payload_hash"] else "MODIFIED"
            results.append({"Event": fp, "Status": status})
        else:
            results.append({"Event": fp, "Status": "REMOVED" if in1 else "ADDED"})

    report = pd.DataFrame(results)
    print("\n" + "="*30 + "\nAUDIT SUMMARY\n" + "="*30)
    print(report["Status"].value_counts())
    
    for status in ["MODIFIED", "UNCHANGED", "ADDED"]:
        subset = report[report["Status"] == status]
        if not subset.empty:
            print(f"\n--- {status} SAMPLE ---")
            debug_event(subset.iloc[0]["Event"], idx1, idx2)
    return report

if __name__ == "__main__":
    run_audit("elijah.json", "cigna_synthetic.json")