import json
import pandas as pd
import networkx as nx
from splink import Linker, DuckDBAPI
from uuid import uuid4
import sys

# --- UTILS ---
def clean_id(raw_id):
    if raw_id is None or pd.isna(raw_id) or str(raw_id).lower() == 'nan' or str(raw_id).strip() == "":
        return "MISSING"
    return str(raw_id).split('/')[-1].strip()

def get_val(obj, path, default="N/A"):
    """Safely extract nested values from FHIR dicts."""
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

# --- STAGE 1: FEATURE EXTRACTION (Using your specific Identifiers) ---
def extract_features(data, source_name):
    rows = []
    for item in data:
        res_type = item.get('resourceType')
        if res_type == "Provenance": continue

        internal_id = clean_id(item.get('id'))
        # Determine Patient Reference
        raw_ref = (item.get('subject', {}).get('reference') or 
                   item.get('patient', {}).get('reference') or 
                   item.get('subject', {}).get('display') or "")
        patient_ref = clean_id(raw_ref)
        
        # Base clinical data
        base = {
            "linkage_id": f"{source_name}_{internal_id}",
            "internal_id": internal_id,
            "source": source_name,
            "resourceType": res_type,
            "patient_ref": patient_ref,
            "payload": "",
            "unique_key": "" # This will hold our specific identifiers
        }

        # --- RESOURCE SPECIFIC LOGIC BASED ON YOUR LIST ---
        
        if res_type == "Patient":
            name = get_val(item, "name.0.family") + " " + get_val(item, "name.0.given.0")
            base["unique_key"] = f"{name}|{get_val(item, 'birthDate')}|{get_val(item, 'gender')}"
            base["payload"] = get_val(item, "address.0.postalCode")

        elif res_type == "AllergyIntolerance":
            base["unique_key"] = f"{get_val(item, 'code.coding.0.code')}|{get_val(item, 'recordedDate')[:10]}"
            base["payload"] = get_val(item, "clinicalStatus.coding.0.code")

        elif res_type == "Observation":
            base["unique_key"] = f"{get_val(item, 'code.coding.0.code')}|{get_val(item, 'effectiveDateTime')}"
            base["payload"] = get_val(item, "valueQuantity.value") or get_val(item, "valueString")

        elif res_type == "Condition":
            # Uses Snomed/ICD10 and period/recordedDate
            date = (item.get('recordedDate') or item.get('onsetDateTime') or "N/A")[:10]
            base["unique_key"] = f"{get_val(item, 'code.coding.0.code')}|{date}"
            base["payload"] = get_val(item, "clinicalStatus.coding.0.code")

        elif res_type == "MedicationRequest":
            # Check for CodeableConcept OR Reference
            m_code = get_val(item, "medicationCodeableConcept.coding.0.code")
            if m_code == "N/A": m_code = clean_id(get_val(item, "medicationReference.reference"))
            base["unique_key"] = f"{m_code}|{get_val(item, 'authoredOn')[:10]}"
            base["payload"] = get_val(item, "status")

        elif res_type == "DiagnosticReport":
            base["unique_key"] = f"{get_val(item, 'performer.0.reference')}|{get_val(item, 'issued')}|{get_val(item, 'encounter.reference')}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Procedure":
            base["unique_key"] = f"{get_val(item, 'performedDateTime')}|{get_val(item, 'location.reference')}|{get_val(item, 'encounter.reference')}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Immunization":
            base["unique_key"] = f"{get_val(item, 'vaccineCode.coding.0.code')}|{get_val(item, 'occurrenceDateTime')[:10]}"
            base["payload"] = get_val(item, "status")

        elif res_type == "Encounter":
            base["unique_key"] = f"{get_val(item, 'serviceProvider.reference')}|{get_val(item, 'location.0.location.reference')}"
            base["payload"] = get_val(item, "period.start", "N/A")

        elif res_type == "DocumentReference":
            base["unique_key"] = f"{get_val(item, 'date')}|{get_val(item, 'type.coding.0.code')}|{get_val(item, 'author.0.reference')}"
            base["payload"] = get_val(item, "status")

        elif res_type == "CarePlan":
            base["unique_key"] = f"{get_val(item, 'created')}|{get_val(item, 'author.identifier.value')}"
            base["payload"] = get_val(item, "status")
            
        elif res_type == "MedicationDispense":
            base["unique_key"] = f"{get_val(item, 'authorizingPrescription.0.reference')}|{get_val(item, 'whenHandedOver')}"
            base["payload"] = get_val(item, "status")

        # Default fallback for types not explicitly parsed (Goal, CareTeam, Location, etc.)
        if not base["unique_key"]:
            base["unique_key"] = f"ID-{internal_id}"
            base["payload"] = "Raw Resource"

        rows.append(base)
    return rows

# --- STAGE 2: IDENTITY RESOLUTION (Splink) ---
def get_patient_matches(df_patients):
    if len(df_patients) < 2: return pd.DataFrame()
    # Logic remains same as previous (probabilistic DOB/Name match)
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "linkage_id",
        "comparisons": [
            {"output_column_name": "unique_key", "comparison_levels": [
                {"sql_condition": "unique_key_l IS NULL OR unique_key_r IS NULL", "is_null_level": True},
                {"sql_condition": "unique_key_l = unique_key_r", "m_probability": 0.99, "u_probability": 0.0001},
                {"sql_condition": "ELSE", "m_probability": 0.01, "u_probability": 0.9999}
            ]}
        ],
        "blocking_rules_to_generate_predictions": []
    }
    db_api = DuckDBAPI()
    linker = Linker(df_patients, settings, db_api)
    return linker.inference.predict(threshold_match_probability=0.90).as_pandas_dataframe()

def create_global_id_map(df_patients, df_matches):
    G = nx.Graph()
    link_to_internal = dict(zip(df_patients['linkage_id'], df_patients['internal_id']))
    for lid in df_patients['linkage_id']: G.add_node(lid)
    if not df_matches.empty:
        for _, row in df_matches.iterrows():
            G.add_edge(row['linkage_id_l'], row['linkage_id_r'])
    
    global_id_map = {}
    for cluster in nx.connected_components(G):
        unique_global_id = f"GLOBAL-{str(uuid4())[:8]}"
        for lid in cluster:
            global_id_map[link_to_internal[lid]] = unique_global_id
    return global_id_map

# --- STAGE 3: EXECUTION ---
def main(file1, file2):
    print("🚀 Starting Refined Identity & Clinical Audit...")
    
    df1 = pd.DataFrame(extract_features(json.load(open(file1)), "file1"))
    df2 = pd.DataFrame(extract_features(json.load(open(file2)), "file2"))

    # 1. Identity Resolution
    df_pats = pd.concat([df1[df1['resourceType']=='Patient'], df2[df2['resourceType']=='Patient']])
    patient_matches = get_patient_matches(df_pats)
    global_id_map = create_global_id_map(df_pats, patient_matches)

    def attach_keys(df):
        # Map patient_ref to the Global Identity
        df['global_id'] = df['patient_ref'].apply(lambda x: global_id_map.get(x, f"UNMAPPED-{x}"))
        # Create final Fingerprint: GlobalID + Type + ResourceSpecificIdentifiers
        df['fingerprint'] = df['global_id'] + "|" + df['resourceType'] + "|" + df['unique_key']
        return df.drop_duplicates(subset=['fingerprint']).set_index('fingerprint')

    idx1 = attach_keys(df1)
    idx2 = attach_keys(df2)

    # 2. Diffing
    all_fps = set(idx1.index) | set(idx2.index)
    changes = []

    for fp in all_fps:
        if "UNMAPPED" in fp and idx1.loc[idx1.index == fp, 'resourceType'].iloc[0] != "Patient":
            continue # Skip records that aren't linked to a verified patient
            
        in_1, in_2 = fp in idx1.index, fp in idx2.index

        if in_1 and not in_2:
            changes.append({"Status": "REMOVED", "Event": fp})
        elif in_2 and not in_1:
            changes.append({"Status": "ADDED", "Event": fp, "Payload": idx2.loc[fp, 'payload']})
        else:
            v1, v2 = str(idx1.loc[fp, 'payload']), str(idx2.loc[fp, 'payload'])
            if v1 != v2:
                changes.append({"Status": "MODIFIED", "Event": fp, "Detail": f"{v1} ➔ {v2}"})
            else:
                changes.append({"Status": "UNCHANGED", "Event": fp})

    # 3. Report
    report = pd.DataFrame(changes)
    print("\n" + "="*80 + "\nAUDIT SUMMARY\n" + "="*80)
    if not report.empty:
        print(report['Status'].value_counts().to_string())
        
        for status in ["MODIFIED", "ADDED", "REMOVED"]:
            subset = report[report['Status'] == status]
            if not subset.empty:
                print(f"\n--- {status} ---")
                for _, r in subset.iterrows():
                    print(f" • {r['Event']}")
                    if status == "MODIFIED": print(f"   Change: {r['Detail']}")
    else:
        print("No clinical data matched.")

if __name__ == "__main__":
    main('elijah.json', 'elijah_2.json')