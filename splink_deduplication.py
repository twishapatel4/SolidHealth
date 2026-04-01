import json, hashlib, re, base64, pandas as pd, networkx as nx
from splink import Linker, DuckDBAPI
from datetime import datetime

# --- [CONFIG & REGISTRY] ---
# Expanded to look deeper into arrays and alternate fields
RESOURCE_MAP = {
    "Observation":        (["code.coding.0.code", "code.coding.1.code", "code.text"], ["effectiveDateTime"]),
    "MedicationRequest":  (["medicationCodeableConcept.coding.0.code", "medicationCodeableConcept.text"], ["authoredOn"]),
    "Condition":          (["code.coding.0.code", "code.coding.1.code", "code.text"], ["onsetDateTime"]),
    "Procedure":          (["code.coding.0.code", "code.text"], ["performedDateTime", "performedPeriod.start"]),
    "Immunization":       (["vaccineCode.coding.0.code", "code.text"], ["occurrenceDateTime"]),
    "Encounter":          (["type.0.coding.0.code", "class.code"], ["period.start"]),
    "Organization":       (["identifier.0.value", "name"], ["STATIC"]),
    "Location":           (["name", "address.city"], ["STATIC"]),
    "Medication":         (["code.coding.0.code", "ingredient.0.itemCodeableConcept.coding.0.code"], ["STATIC"]),
    "Binary":             (["contentType", "STATIC"], ["STATIC"]),
    "DocumentReference":  (["type.coding.0.code", "category.0.coding.0.code"], ["date"]),
    "Claim":              (["type.coding.0.code"], ["billablePeriod.start"]),
    "CarePlan":           (["category.0.coding.0.code", "category.0.text", "STATIC"], ["period.start"]),
    "ExplanationOfBenefit": (["type.coding.0.code"], ["billablePeriod.start", "item.0.servicedDate"]),
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
        # return dt.strftime("%Y-%m-%dT%H:%M")
        return str(date_str)[:10]
    except:
        return str(date_str)[:10] # Fallback to Day level if parsing fails

# --- [DIFFING & DEBUGGING] ---
def deep_clinical_diff(d1, d2, path=""):
    """
    Recursively finds differences between two 'Essence' dictionaries.
    Because we use 'Essence', noise like IDs and Meta are already gone.
    """
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        # If one is a list, normalize to string for comparison
        return f"{d1} ➔ {d2}" if d1 != d2 else None

    diffs = []
    # Check all keys from both versions of the clinical essence
    for k in (set(d1.keys()) | set(d2.keys())):
        v1, v2 = d1.get(k), d2.get(k)
        
        if v1 != v2:
            current_path = f"{path}.{k}" if path else k
            
            # If both are dictionaries, go deeper
            if isinstance(v1, dict) and isinstance(v2, dict):
                sub_diffs = deep_clinical_diff(v1, v2, current_path)
                if sub_diffs:
                    diffs.append(sub_diffs)
            # If they are lists (like multiple codings or instructions)
            elif isinstance(v1, list) and isinstance(v2, list):
                diffs.append(f"{current_path}: {v1} ➔ {v2}")
            # Base case: Value change (e.g., status: 'active' -> 'completed')
            else:
                diffs.append(f"{current_path}: [{v1}] ➔ [{v2}]")
                
    return ", ".join(filter(None, diffs))

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
            print(f"  Diff: {deep_clinical_diff(r1['essence'], r2['essence'])}")

def normalize_clinical_unit(u):
    """Normalizes units to be platform independent."""
    if not u: return ""
    # Remove all non-alphanumeric characters and lowercase
    # e.g., "mg/dL" -> "mgdl", "Milligrams / Deciliter" -> "milligramsdeciliter"
    u = re.sub(r'[^a-z0-9]', '', str(u).lower())
    
    # Common Synonym Mapping
    synonyms = {
        "milligramperdeciliter": "mgdl",
        "milliliter": "ml",
        "millilitre": "ml",
        "beatsperminute": "bpm",
        "countpermin": "bpm",
        "percent": "pct"
    }
    return synonyms.get(u, u)

def get_clinical_essence(obj):
    # Technical noise to ignore
    nuke_keys = {
        'id', 'meta', 'text', 'fullUrl', 'url',
        'userSelected', 'versionId', 'lastUpdated',
        'assigner', 'period', 'use' ,'identifier','batch','masterIdentifier'
    }
    
    if isinstance(obj, dict):
        # 1. Binary Content (Hash actual data)
        if obj.get("resourceType") == "Binary" and "data" in obj:
            return hashlib.sha256(str(obj["data"]).encode()).hexdigest()[:10]

        # 2. Quantity Handling (Value + Unit)
        # We must return this BEFORE the "Coding" check to avoid losing the numeric value
        if "value" in obj and ("unit" in obj or "code" in obj):
            val = round(float(obj["value"]), 4) if obj["value"] is not None else 0
            # Normalize the unit string
            unit_raw = obj.get("unit") or obj.get("code") or ""
            unit_clean = re.sub(r'[^a-z0-9]', '', str(unit_raw).lower())
            return f"{val}{unit_clean}"

        # 3. Identifiers (Value + Short System)
        if "value" in obj and "system" in obj:
            sys_short = str(obj["system"]).split('/')[-1].split(':')[-1]
            return f"{sys_short}|{obj['value']}".lower()

        # 4. References (Type + Display Name)
        if "reference" in obj:
            ref_type = str(obj["reference"]).split('/')[0].lower()
            if "display" in obj:
                clean_display = re.sub(r'[^a-z0-9]', '', str(obj["display"]).lower())
                return f"{ref_type}:{clean_display}"
            return ref_type

        # 5. Codings (System + Code)
        if "code" in obj and "system" in obj:
            sys_short = str(obj["system"]).split('/')[-1].split(':')[-1]
            return f"{sys_short}|{obj['code']}".lower()
        
        if "city" in obj or "postalCode" in obj or "line" in obj:
            cleaned_addr = {}
            for k, v in obj.items():
                if k in ['use', 'id', 'extension']: continue # Ignore technical address metadata
                # Remove spaces and punctuation from address parts
                # "80 SEYMOUR STREET" -> "80seymourstreet"
                if isinstance(v, str):
                    cleaned_addr[k] = re.sub(r'[^a-z0-9]', '', v.lower())
                elif isinstance(v, list):
                    cleaned_addr[k] = [re.sub(r'[^a-z0-9]', '', str(i).lower()) for i in v]
            return cleaned_addr
        # General Recursion
        cleaned = {}
        for k, v in obj.items():
            if k in nuke_keys: continue
            val = get_clinical_essence(v)
            if val is not None and val != "" and val != [] and val != {}:
                cleaned[k] = val
        return cleaned
    
    if isinstance(obj, list):
        items = [get_clinical_essence(x) for x in obj if x is not None]
        items = [i for i in items if i != {} and i != [] and i != ""]
        try:
            return sorted(items, key=lambda x: str(x))
        except:
            return items
            
    if isinstance(obj, str):
        # 6. Date/Time Rounding (The YYYY-MM-DD Fix)
        # Matches 2023-01-01... and returns just 2023-01-01
        if re.match(r'^\d{4}-\d{2}-\d{2}', obj):
            return obj[:10]
            
        # URL stripping for remaining strings
        if obj.startswith('http://') or obj.startswith('https://'):
            return obj.split('/')[-1].lower()
        return obj.strip().lower()
        
    return obj

def extract_resource_data(item):
    res_type = item.get("resourceType")
    if res_type in ["Provenance", None]: return None
    
    if res_type == "Binary":
        identity_code = "FILE"
        date_val = "STATIC"
    

    biz_id = None
    identifiers = item.get("identifier", [])
    if isinstance(identifiers, list):
        for ident in identifiers:
            # Check if this identifier is an NPI
            system = str(ident.get("system", "")).lower()
            if "us-npi" in system or "npi" in system:
                biz_id = ident.get("value")
                break
  
    # Fallback to standard logic if NPI not found
    if not biz_id:
        biz_id = get_val(item, "masterIdentifier.value") or get_val(item, "identifier.0.value")
    # --------------------------------------------

    code_paths, date_paths = RESOURCE_MAP.get(res_type, DEFAULT_PATHS)
    
    # Logic: Find the first path that returns a non-null value
    identity_code = next((get_val(item, p) for p in code_paths if get_val(item, p)), "NOCODE")
    date_val = next((get_val(item, p) for p in date_paths if get_val(item, p)), "NODATE")
    sort_time = str(date_val).replace(' ', 'T')[:16] if date_val not in ["STATIC", "NODATE"] else "0000"
    # biz_id = get_val(item, "identifier.0.value") or get_val(item, "identifier.value")
    
    essence = get_clinical_essence(item)
    essence_sig = json.dumps(essence, sort_keys=True)
    return {
        "internal_id": clean_ref(item.get("id")),
        "business_id": str(biz_id) if biz_id else None,
        "res_type": res_type,
        "patient_ref": clean_ref(get_val(item, "subject.reference") or get_val(item, "patient.reference")),
        "code": normalize(identity_code),
        "date_key": normalize_date(date_val),
        "sort_time": sort_time,               # This is Minute-level (YYYY-MM-DDTHH:MM)
        "essence_sig": essence_sig,
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

def process_fingerprints(data_list, gid_map):
    if not data_list: return pd.DataFrame()
    df = pd.DataFrame(data_list)
    
    # Map GID
    df["gid"] = df["patient_ref"].map(gid_map).fillna(df["internal_id"].map(gid_map)).fillna("SYSTEM")
    
    # FINGERPRINT: This is how we match records between files.
    # It must NOT change when the clinical value (54.0 -> 52.0) changes.
    df["fp_base"] = df["gid"] + "|" + df["res_type"] + "|" + df["code"] + "|" + df["date_key"]

    # SORTING: We sort by internal_id to keep the sequence stable.
    # If we sorted by payload_hash here, a change in value would change the sequence number.
    # df = df.sort_values(["fp_base", "internal_id"])
    df = df.sort_values(["fp_base", "sort_time", "essence_sig"])
    df["seq"] = df.groupby(["fp_base"]).cumcount() + 1
    
    df["final_fp"] = df["fp_base"] + "|seq-" + df["seq"].astype(str)
    
    return df.drop_duplicates(subset=["final_fp"]).set_index("final_fp")

def run_audit(file1, file2):
    raw1 = [extract_resource_data(i) for i in json.load(open(file1)) if extract_resource_data(i)]
    raw2 = [extract_resource_data(i) for i in json.load(open(file2)) if extract_resource_data(i)]
    
    df_full = pd.concat([pd.DataFrame(raw1), pd.DataFrame(raw2)])
    gid_map = get_gid_map(df_full)
    
    idx1 = process_fingerprints(raw1, gid_map)
    idx2 = process_fingerprints(raw2, gid_map)
    
    # --- [PATIENT IDENTITY GUARD] ---
    # Get the sets of Global IDs present in each file
    # We ignore "SYSTEM" (records without a patient reference) for this check
    gids1 = set(idx1["gid"].unique()) - {"SYSTEM"}
    gids2 = set(idx2["gid"].unique()) - {"SYSTEM"}
    
    # Check if there is any overlap between the two files
    common_patients = gids1.intersection(gids2)
    
    if not common_patients:
        print("\n" + "!"*50)
        print("❌ COMPARISON ABORTED: Different Patients Detected")
        print(f"File 1 Patient(s): {list(gids1)}")
        print(f"File 2 Patient(s): {list(gids2)}")
        print("Clinical logic requires at least one matching patient to proceed.")
        print("!"*50 + "\n")
        return None
    # ---------------------------------

    results = []
    all_fps = sorted(set(idx1.index) | set(idx2.index))
    for fp in all_fps:
        if "|Patient|" in fp: continue
        in1, in2 = fp in idx1.index, fp in idx2.index
        
        if in1 and in2:
            h1 = idx1.loc[fp, "payload_hash"]
            h2 = idx2.loc[fp, "payload_hash"]
            if isinstance(h1, pd.Series): h1 = h1.iloc[0]
            if isinstance(h2, pd.Series): h2 = h2.iloc[0]
            
            status = "UNCHANGED" if h1 == h2 else "MODIFIED"
            results.append({"Event": fp, "Status": status})
        else:
            results.append({"Event": fp, "Status": "REMOVED" if in1 else "ADDED"})

    report = pd.DataFrame(results)
    print("\n" + "="*30 + "\nAUDIT SUMMARY\n" + "="*30)
    print(report["Status"].value_counts())
    
    mod_subset = report[report["Status"] == "MODIFIED"]
    if not mod_subset.empty:
        # debug_event(mod_subset.iloc[0]["Event"], idx1, idx2)
        num_samples = min(len(mod_subset), 3)
        random_samples = mod_subset.sample(n=num_samples)
        
        print(f"\n🎲 Inspecting {num_samples} random MODIFIED records:")
        for _, row in random_samples.iterrows():
            debug_event(row["Event"], idx1, idx2)
        
    return report

if __name__ == "__main__":
    run_audit("elijah.json", "cigna_synthetic.json")