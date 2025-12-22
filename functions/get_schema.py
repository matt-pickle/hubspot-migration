import requests
import time

def get_schema(object_type, HS_KEY, retry):
    url = f"https://api.hubapi.com/crm/v3/schemas/{object_type}"
    headers = { "Authorization": f"Bearer {HS_KEY}", "Content-Type": "application/json" }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        print(f"Fetched HubSpot {object_type} schema")
        time.sleep(0.25)
        return response_json.get("properties", [])
    except requests.exceptions.RequestException as e:
        if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            print(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            get_schema(object_type, HS_KEY, retry)
        elif retry == 5:
            print("Max retries reached. Skipping schema fetch.")
        else:
            error_msg = e.response.text if e.response is not None and e.response.text else str(e)
            print(f"Error fetching HubSpot {object_type} schema: {error_msg}")