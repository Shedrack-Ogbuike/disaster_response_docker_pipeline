import requests
import json

def debug_fema_api():
    """Debug FEMA API to see actual field names"""
    print("🔍 Debugging FEMA API Field Names...")
    
    # Test Disaster Declarations endpoint
    print("\n=== DISASTER DECLARATIONS API ===")
    declarations_url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    params = {"$top": 2}
    
    try:
        response = requests.get(declarations_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'DisasterDeclarationsSummaries' in data and data['DisasterDeclarationsSummaries']:
            sample_record = data['DisasterDeclarationsSummaries'][0]
            print("✅ First declaration record fields:")
            for key, value in sample_record.items():
                print(f"   '{key}': {value}")
        else:
            print("❌ No declaration data received")
            
    except Exception as e:
        print(f"❌ Declaration API error: {e}")
    
    # Test Public Assistance endpoint
    print("\n=== PUBLIC ASSISTANCE API ===")
    assistance_url = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"
    params = {"$top": 2}
    
    try:
        response = requests.get(assistance_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'PublicAssistanceFundedProjectsDetails' in data and data['PublicAssistanceFundedProjectsDetails']:
            sample_record = data['PublicAssistanceFundedProjectsDetails'][0]
            print("✅ First public assistance record fields:")
            for key, value in sample_record.items():
                print(f"   '{key}': {value}")
        else:
            print("❌ No public assistance data received")
            
    except Exception as e:
        print(f"❌ Public Assistance API error: {e}")

if __name__ == "__main__":
    debug_fema_api()