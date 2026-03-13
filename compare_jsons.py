import json
from collections import defaultdict

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def compare_jsons(file1, file2):
    data1 = load_json(file1)
    data2 = load_json(file2)

    print(f"File 1 ({file1}): {len(data1)} items")
    print(f"File 2 ({file2}): {len(data2)} items")

    # Create dicts by id
    dict1 = {item['id']: item for item in data1}
    dict2 = {item['id']: item for item in data2}

    ids1 = set(dict1.keys())
    ids2 = set(dict2.keys())

    only_in_1 = ids1 - ids2
    only_in_2 = ids2 - ids1
    common = ids1 & ids2

    print(f"Items only in {file1}: {len(only_in_1)}")
    print(f"Items only in {file2}: {len(only_in_2)}")
    print(f"Common items: {len(common)}")

    # For common items, check if they are identical
    differences = []
    for id_ in common:
        if dict1[id_] != dict2[id_]:
            differences.append(id_)

    print(f"Common items with differences: {len(differences)}")

    if differences:
        print("IDs with differences:", differences)  # Show first 10

    # Count resource types
    types1 = defaultdict(int)
    types2 = defaultdict(int)
    for item in data1:
        types1[item.get('resourceType', 'Unknown')] += 1
    for item in data2:
        types2[item.get('resourceType', 'Unknown')] += 1

    print("\nResource types in File 1:")
    for t, c in sorted(types1.items()):
        print(f"  {t}: {c}")

    print("\nResource types in File 2:")
    for t, c in sorted(types2.items()):
        print(f"  {t}: {c}")

    # Show added resources
    added = [dict2[id_] for id_ in only_in_2]
    added_types = defaultdict(int)
    for item in added:
        added_types[item.get('resourceType', 'Unknown')] += 1

    print("\nAdded resources in File 2:")
    for t, c in sorted(added_types.items()):
        print(f"  {t}: {c}")

    # If same patient, perhaps check if it's the same patient id
    patient_ids = set()
    for item in data1 + data2:
        if item.get('resourceType') == 'Patient':
            patient_ids.add(item['id'])

    print(f"\nUnique patient IDs: {patient_ids}")

if __name__ == "__main__":
    file1 = 'user_36ry42FouyaK8HbVxGcqZKAPqLR_20260310T110622.json'
    file2 = 'user_36ry42FouyaK8HbVxGcqZKAPqLR_20260310T111415.json'
    compare_jsons(file1, file2)