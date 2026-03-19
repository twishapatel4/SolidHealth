import json
import pandas as pd
import networkx as nx
# from splink.duckdb.linker import DuckDBLinker
# import splink.duckdb.comparison_library as cl
from splink import Linker, DuckDBAPI
import splink.comparison_library as cl
from uuid import uuid4

# --- STAGE 1: FEATURE EXTRACTION & NORMALIZATION ---
def extract_features(data, source_name):
    """Flattens FHIR JSON into a list of dictionaries with normalized keys."""
    rows = []
    for item in data:
        res_type = item.get('resourceType')
        base = {
            "internal_id": item.get('id'),
            "source": source_name,
            "resourceType": res_type
        }

        if res_type == "Patient":
            base.update({
                "name": (item.get('name', [{}])[0].get('family', '') + " " + 
                         "".join(item.get('name', [{}])[0].get('given', []))).upper().strip(),
                "dob": item.get('birthDate', ''),
                "gender": item.get('gender', ''),
                "zip": item.get('address', [{}])[0].get('postalCode', '')
            })
        
        elif res_type == "Observation":
            # Semantic Link: Patient + Code + Date
            base.update({
                "patient_ref": item.get('subject', {}).get('reference'),
                "code": item.get('code', {}).get('coding', [{}])[0].get('code'),
                "date": item.get('effectiveDateTime', '')[:10], # Truncate to Date
                "value": str(item.get('valueQuantity', {}).get('value', ''))
            })

        elif res_type == "Condition":
            # Semantic Link: Patient + SNOMED/ICD Code
            base.update({
                "patient_ref": item.get('subject', {}).get('reference'),
                "code": item.get('code', {}).get('coding', [{}])[0].get('code'),
                "category": item.get('category', [{}])[0].get('coding', [{}])[0].get('code')
            })

        elif res_type == "MedicationRequest":
            base.update({
                "patient_ref": item.get('subject', {}).get('reference'),
                "rxnorm": item.get('medicationCodeableConcept', {}).get('coding', [{}])[0].get('code'),
                "date": item.get('authoredOn', '')[:10]
            })

        # Add more logic for Procedure, Immunization etc. based on your list
        rows.append(base)
    return rows

# --- STAGE 2: PROBABILISTIC PATIENT MATCHING (SPLINK) ---
def get_patient_matches(df_patients):
    """Uses Fellegi-Sunter via Splink to find duplicate patients."""
    
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("dob"),
            cl.LevenshteinAtThresholds("name", 2),
            cl.ExactMatch("gender"),
            cl.ExactMatch("zip"),
        ],
        "blocking_rules_to_generate_predictions": [
            "l.dob = r.dob",
            "l.name = r.name"
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": False
    }

    # linker = DuckDBAPI(df_patients, settings)
    # # In production, you would 'train' these weights. For now, we estimate.
    # linker.estimate_u_using_random_sampling(max_pairs=1e6)
    
    # # Calculate matches with high confidence (Probability > 0.90)
    # df_matches = linker.predict(threshold_match_probability=0.90).as_pandas_dataframe()
    db_api = DuckDBAPI()

    # 2. Initialize the Linker (Data + Settings + API)
    # df_patients is the pandas dataframe
    linker = Linker(df_patients, settings, db_api)
    
    # 3. Estimate weights (Using the v4 'training' namespace)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    
    # 4. Predict (Using the v4 'inference' namespace)
    # .as_pandas_dataframe() is still used to convert the result
    df_matches = linker.inference.predict(threshold_match_probability=0.90).as_pandas_dataframe()
    return df_matches

# --- STAGE 3: GRAPH CLUSTERING (NETWORKX) ---
def create_global_ids(df_patients, df_matches):
    """Uses Graph Theory to group all related Patient IDs into one Global UUID."""
    G = nx.Graph()
    
    # Add all unique internal IDs as nodes
    for _, row in df_patients.iterrows():
        G.add_node(row['internal_id'])
        
    # Add edges for matches found by Splink
    for _, row in df_matches.iterrows():
        G.add_edge(row['internal_id_l'], row['internal_id_r'])
        
    # Find clusters (Connected Components)
    global_id_map = {}
    for cluster in nx.connected_components(G):
        unique_id = f"GLOBAL-PATIENT-{str(uuid4())[:8]}"
        for internal_id in cluster:
            global_id_map[internal_id] = unique_id
            
    return global_id_map

# --- MAIN EXECUTION PIPELINE ---
def main(file1, file2):
    # 1. Load and Flatten
    raw_data1 = json.load(open(file1))
    raw_data2 = json.load(open(file2))
    
    df_all = pd.DataFrame(extract_features(raw_data1, "file1") + extract_features(raw_data2, "file2"))
    
    # 2. Separate Patients for Splink
    df_patients = df_all[df_all['resourceType'] == 'Patient'].copy()
    df_patients['unique_id'] = range(len(df_patients)) # Splink needs a numeric unique row ID
    
    # 3. Probabilistic Linkage
    print("Running Probabilistic Matching...")
    patient_matches = get_patient_matches(df_patients)
    
    # 4. Generate Global IDs
    print("Generating Global Identity Graph...")
    global_id_map = create_global_ids(df_patients, patient_matches)
    
    # 5. Apply Global IDs to Clinical Data
    # Map the local Patient References (e.g. 'Patient/123') to Global IDs
    df_clinical = df_all[df_all['resourceType'] != 'Patient'].copy()
    
    def resolve_patient(ref):
        # clean_ref = str(ref).replace('Patient/', '')
        # In your extract_features loop:
        if not ref or str(ref) == 'nan':
            return "ORPHAN-RECORD"
        
        # 'ref' is the value from the 'patient_ref' column (e.g., "Patient/123")
        # Split by '/' and take the last part (the actual ID)
        clean_patient_id = str(ref).split('/')[-1]
        return global_id_map.get(clean_patient_id, "ORPHAN-RECORD")

    df_clinical['global_patient_id'] = df_clinical['patient_ref'].apply(resolve_patient)

    # 6. Final Deduplication (Semantic Fingerprinting)
    # Group by Patient + Type + Code + Date to find clinical duplicates
    df_clinical['fingerprint'] = df_clinical.apply(
        lambda x: f"{x['global_patient_id']}|{x['resourceType']}|{x.get('code', 'N/A')}|{x.get('date', 'N/A')}|{x.get('value', 'N/A')}", 
        axis=1
    )

    duplicates = df_clinical[df_clinical.duplicated('fingerprint', keep=False)]
    
    print(f"\n--- DEDUPLICATION RESULTS ---")
    print(f"Total Patient Records: {len(df_patients)}")
    print(f"Unique Global Patients Found: {len(set(global_id_map.values()))}")
    print(f"Duplicate Clinical Records Found: {len(duplicates)}")
    
    # Output Example:
    if not duplicates.empty:
        print("\nSample Duplicates (Same event, different IDs):")
        print(duplicates[['internal_id', 'source', 'resourceType', 'fingerprint']].head(10))

if __name__ == "__main__":
    main('user_36ry42FouyaK8HbVxGcqZKAPqLR_20260310T110622.json', 
         'new_data.json')