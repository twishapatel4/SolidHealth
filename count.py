import json
from collections import Counter

def count_resources(filename):
    data = json.load(open(filename))
    return Counter([item['resourceType'] for item in data])

print("File 1 (Aetna):", count_resources('elijah.json'))
print("File 2 (Cigna):", count_resources('cigna_synthetic.json'))