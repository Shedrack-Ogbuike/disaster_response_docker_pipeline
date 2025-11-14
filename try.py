import requests
import json

url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
params = {"$top": 5}

response = requests.get(url, params=params)
print("Status Code:", response.status_code)
print("Headers:", response.headers['content-type'])

data = response.json()

print("\n=== RAW API RESPONSE ===")
print(json.dumps(data, indent=2))

if "DisasterDeclarationsSummaries" in data:
    records = data["DisasterDeclarationsSummaries"]
    print(f"\n=== FIRST RECORD ===")
    first_record = records[0] if records else {}
    for key, value in first_record.items():
        print(f"{key}: {value} (type: {type(value).__name__})")