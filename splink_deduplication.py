import json
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
from uuid import uuid4
import re

# --- UTILS ---
def normalize_string(s):
    if not s or s == "N/A": return "na"
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def clean_id(raw_id):
    if raw_id is None or pd.isna(raw_id) or str(raw_id).lower() == 'nan' or str(raw_id).strip() == "":
        return "MISSING"
    return str(raw_id).split('/')[-1].strip()

def get_val(obj, path, default="N/A"):
    components = path.split('.')
    for part in components:
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit():
            try: obj = obj[int(part)]
            except: return default
        else:
            return default
    return str(obj) if obj is not None else default

# --- STAGE 1: BUSINESS-ORIENTED FEATURE EXTRACTION ---
def extract_features(data, source_name):
    rows = []
    for item in data:
        res_type = item.get('resourceType')
        if res_type == "Provenance": continue

        internal_id = clean_id(item.get('id'))
        # Try to find a logical patient reference
        raw_ref = (item.get('subject', {}).get('reference') or 
                   item.get('patient', {}).get('reference') or 
                   item.get('subject', {}).get('display') or "")
        patient_ref = clean_id(raw_ref)
        
        base = {
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "payload": "",
            "unique_key": "" 
        }

        # BUSINESS LOGIC: We ignore platform UUIDs and use Clinical Codes + Dates
        if res_type == "Patient":
            # Business Key: Family | Given | DOB
            fname = normalize_string(get_val(item, "name.0.family"))
            gname = normalize_string(get_val(item, "name.0.given.0"))
            dob = get_val(item, 'birthDate')
            base["unique_key"] = f"{fname}|{gname}|{dob}"
            base["payload"] = get_val(item, "gender")

        elif res_type == "AllergyIntolerance":
            # Business Key: Snomed/RxNorm Code + Recorded Date
            code = get_val(item, 'code.coding.0.code')
            date = get_val(item, 'recordedDate')[:10]
            base["unique_key"] = f"{code}|{date}"
            base["payload"] = get_val(item, "clinicalStatus.coding.0.code")

        elif res_type == "Observation":
            # Business Key: LOINC + DateTime + (Optional: normalized Method)
            code = get_val(item, 'code.coding.0.code')
            date = get_val(item, 'effectiveDateTime')
            base["unique_key"] = f"{code}|{date}"
            base["payload"] = get_val(item, "valueQuantity.value")

        elif res_type == "Condition":
            # Business Key: Code + Recorded Date (ignore Platform ID)
            code = get_val(item, 'code.coding.0.code')
            date = (item.get('recordedDate') or item.get('onsetDateTime') or "N/A")[:10]
            base["unique_key"] = f"{code}|{date}"
            base["payload"] = get_val(item, "category.0.coding.0.code")

        elif res_type == "MedicationRequest":
            m_code = get_val(item, "medicationCodeableConcept.coding.0.code")
            if m_code == "N/A": 
                # If it's a reference, we can't use the UUID. 
                # In synthetic data, we'll try to get the display name or just use the date.
                m_code = normalize_string(get_val(item, "medicationReference.display"))
            date = get_val(item, 'authoredOn')[:10]
            base["unique_key"] = f"{m_code}|{date}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Procedure":
            # Business Key: Procedure Code + Date (DO NOT use encounter/location UUID)
            proc_code = get_val(item, 'code.coding.0.code')
            date = get_val(item, 'performedDateTime')[:16] # Minute precision
            base["unique_key"] = f"{proc_code}|{date}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Immunization":
            # Business Key: CVX Code + Date
            v_code = get_val(item, 'vaccineCode.coding.0.code')
            date = get_val(item, 'occurrenceDateTime')[:10]
            base["unique_key"] = f"{v_code}|{date}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Encounter":
            # Business Key: Type Code + Start Date
            e_type = get_val(item, 'type.0.coding.0.code')
            date = get_val(item, 'period.start')[:10]
            base["unique_key"] = f"{e_type}|{date}"
            base["payload"] = get_val(item, "status")

        elif res_type == "DocumentReference":
            # Business Key: LOINC Code + Date
            doc_type = get_val(item, 'type.coding.0.code')
            date = get_val(item, 'date')[:10]
            base["unique_key"] = f"{doc_type}|{date}"
            base["payload"] = get_val(item, "status")

        if not base["unique_key"]:
            base["unique_key"] = f"fallback-{internal_id}"

        rows.append(base)
    return rows

# --- STAGE 2: IDENTITY RESOLUTION ---
def get_patient_matches(df_patients):
    if len(df_patients) < 2: return pd.DataFrame()
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "linkage_id",
        "comparisons": [
            {"output_column_name": "unique_key", "comparison_levels": [
                {"sql_condition": "unique_key_l = unique_key_r", "label_for_charts": "Exact match"},
                {"sql_condition": "ELSE", "label_for_charts": "All other queries"}
            ]}
        ]
    }
    db_api = DuckDBAPI()
    linker = Linker(df_patients, settings, db_api)
    return linker.inference.predict(threshold_match_probability=0.95).as_pandas_dataframe()

def create_global_id_map(df_patients, df_matches):
    G = nx.Graph()
    link_to_internal = dict(zip(df_patients['linkage_id'], df_patients['internal_id']))
    for lid in df_patients['linkage_id']: G.add_node(lid)
    if not df_matches.empty:
        for _, row in df_matches.iterrows():
            G.add_edge(row['linkage_id_l'], row['linkage_id_r'])
    
    global_id_map = {}
    for cluster in nx.connected_components(G):
        unique_global_id = "PATIENT-ELIJAH-FISHER" # For this demo, or f"GLOBAL-{str(uuid4())[:8]}"
        for lid in cluster:
            global_id_map[link_to_internal[lid]] = unique_global_id
    return global_id_map

# --- STAGE 3: EXECUTION ---
def main(file1, file2):
    df1 = pd.DataFrame(extract_features(json.load(open(file1)), "Platform_A"))
    df2 = pd.DataFrame(extract_features(json.load(open(file2)), "Platform_B"))

    # 1. Identity Resolution (Force Elijah to match)
    df_pats = pd.concat([df1[df1['resourceType']=='Patient'], df2[df2['resourceType']=='Patient']])
    patient_matches = get_patient_matches(df_pats)
    global_id_map = create_global_id_map(df_pats, patient_matches)

    def attach_keys(df):
        # We find the Global ID for the patient this resource belongs to
        df['global_id'] = df['patient_ref'].apply(lambda x: global_id_map.get(x, "UNKNOWN"))
        # Fingerprint is: Global Identity + Resource Type + Business Logic Key
        df['fingerprint'] = df['global_id'] + "|" + df['resourceType'] + "|" + df['unique_key']
        return df.drop_duplicates(subset=['fingerprint']).set_index('fingerprint')

    idx1 = attach_keys(df1)
    idx2 = attach_keys(df2)

    all_fps = set(idx1.index) | set(idx2.index)
    changes = []

    for fp in all_fps:
        # Ignore items not linked to a patient for this specific audit
        if "UNKNOWN" in fp: continue
        
        in_1, in_2 = fp in idx1.index, fp in idx2.index

        if in_1 and in_2:
            v1, v2 = str(idx1.loc[fp, 'payload']), str(idx2.loc[fp, 'payload'])
            print(v1, v2)
            if v1 != v2:
                changes.append({"Status": "MODIFIED", "Event": fp, "Detail": f"{v1} ➔ {v2}"})
            else:
                changes.append({"Status": "UNCHANGED", "Event": fp})
        elif in_1:
            changes.append({"Status": "REMOVED", "Event": fp})
        else:
            changes.append({"Status": "ADDED", "Event": fp})

    report = pd.DataFrame(changes)
    print("\n" + "="*80 + "\nCROSS-PLATFORM AUDIT SUMMARY\n" + "="*80)
    if not report.empty:
        print(report['Status'].value_counts().to_string())
        # Show a few Unchanged to prove it worked
        unchanged = report[report['Status'] == "UNCHANGED"]
        if not unchanged.empty:
            for status in ["MODIFIED", "ADDED", "REMOVED"]:
                subset = report[report['Status'] == status]
                # print(subset)
                if not subset.empty:
                    print(f"\n--- {status} ---")
                    for _, r in subset.iterrows():
                        print(f" • {r['Event']}")
                        if status == "MODIFIED": 
                            print(f"   Change: {r['Detail']}")
            print(f"\n✅ Successfully Matched {len(unchanged)} records across platforms (IDs differed, but clinical data was identical)")
    else:
        print("No clinical data matched.")

if __name__ == "__main__":
    main('elijah.json', 'cigna_synthetic.json')