import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import time
import sys
from functions.parse_csv import parse_csv
from functions.logger import log, output_logs
from functions.search_records import SearchBody, search_records

load_dotenv()
PRIVATE_APP_KEY = os.getenv("PRIVATE_APP_KEY")
DEAL_EXT_ID = os.getenv("DEAL_EXT_ID")
CONTACT_EXT_ID = os.getenv("CONTACT_EXT_ID")
COMPANY_EXT_ID = os.getenv("COMPANY_EXT_ID")
if not PRIVATE_APP_KEY or not CONTACT_EXT_ID or not COMPANY_EXT_ID or not DEAL_EXT_ID:
    log("Error: Missing required environment variable(s).")
    sys.exit()
hs_headers = { "Authorization": f"Bearer {PRIVATE_APP_KEY}" }

object_type = sys.argv[1] if len(sys.argv) > 1 else None
if not object_type:
    log("Error: Object type not provided as argument.")
    sys.exit()

assoc_type_id = 0
if object_type == "deal" or object_type == "deals":
    assoc_type_id = 214
    object_type = "deals"
elif object_type == "contact" or object_type == "contacts": 
    assoc_type_id = 203
    object_type = "contacts"
elif object_type == "company" or object_type == "companies":
    assoc_type_id = 204
    object_type = "companies"
else:
    log("Error: Invalid object type provided. Must be one of 'deals', 'contacts', or 'companies'.")
    sys.exit()

source_dir = Path(__file__).parent.parent / "files" / object_type
file_dict = {}

for root, dirs, files in os.walk(source_dir):
    root_path = Path(root)
    
    # Skip if this is the source directory
    if root_path == source_dir:
        continue
    
    opportunity_id = root_path.name

    if opportunity_id not in file_dict.keys():
        file_dict[opportunity_id] = []
    
    for filename in files:
        # Skip hidden files
        if filename.startswith('.'):
            continue
        
        path = root_path / filename
        file = {
            "path": root_path / filename,
            "name": filename,
            "file": open(path, 'rb')
        }
        file_dict[opportunity_id].append(file)

    log(f"Processed folder: {opportunity_id}, found {len(files)} files.")

folder_names = list(file_dict.keys())
log(f"Total opportunities with files: {len(file_dict.keys())}")
log(f"Total files to migrate: {sum(len(v) for v in file_dict.values())}")

# Get Record IDs
records = []
for i in range(0, len(folder_names), 100):
    batch = folder_names[i:i+100]
    search_body: SearchBody = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": DEAL_EXT_ID,
                        "operator": "IN",
                        "values": batch
                    }
                ]
            }
        ],
        "properties": [DEAL_EXT_ID],
        "limit": 100
    }
    records.append(search_records(object_type, search_body, PRIVATE_APP_KEY))
    time.sleep(0.25)
log(f"Total records found: {len(records)}")
        
# Create Folders in HubSpot File Manager
def create_folder(name, retry):
    url = f"https://api.hubapi.com/files/v3/folders"
    data = {
        "name": name,
        "parentPath": f"/Migrated Files/{object_type}",
    }
    try:
        response = requests.post(url, headers=hs_headers, json=data)
        response.raise_for_status()
        response_json = response.json()
        log(f"Created folder {response_json.get("name")}")
        return response_json.get("id")
    except requests.exceptions.RequestException as e:
        if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            create_folder(name, retry)
        elif retry == 5:
            log(f"Max retries reached. Skipping folder {name}.")
        else:
            error_msg = e.response.text if e.response is not None and e.response.text else str(e)
            log(f"Error creating folder {name}: {error_msg}")

# Upload Files
hs_files = []
def upload_file(file, folder_id, retry):
    url = f"https://api.hubapi.com/files/v3/files"
    files = { "file": (file["name"], open(file["path"], "rb")) }
    data = {
        "fileName": file["name"],
        "folderId": folder_id,
        "options": '{ "access": "PRIVATE" }',
    }
    try:
        response = requests.post(url, headers=hs_headers, data=data, files=files)
        response.raise_for_status()
        response_json = response.json()
        log(f"Uploaded file {response_json.get("name")}")
        hs_files.append(response_json)
        return response_json.get("id")
    except requests.exceptions.RequestException as e:
        if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            upload_file(file, folder_id, retry)
        elif retry == 5:
            log(f"Max retries reached. Skipping file {file.get("name")}.")
        else:
            error_msg = e.response.text if e.response is not None and e.response.text else str(e)
            log(f"Error uploading file {file.get("name")}: {error_msg}")

note_inputs = []
for i, folder_name in enumerate(folder_names):
    log(f"\n\nPROCESSING FOLDER {i + 1}/{len(folder_names)}")
    folder_id = create_folder(folder_name, 0)
    time.sleep(0.25)
    file_ids = []
    for file in file_dict[folder_name]:
        file_id = upload_file(file, folder_id, 0)
        file_ids.append(file_id)
        time.sleep(0.25)
    log(f"Total files uploaded: {len(hs_files)}")
    record_id = next((record["id"] for record in records if record["properties"].get(DEAL_EXT_ID) == folder_name), None)
    if record_id:
        note_input = {
            "properties": {
                "hs_note_body": f"Migrated files for {object_type} {folder_name}",
                "hs_attachment_ids": ";".join(file_ids)
            },
            "associations": [{
                "to": { "id": record_id },
                "types": [{
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": assoc_type_id
                }]
            }]
        }
        note_inputs.append(note_input)
    else:
        log(f"No Record ID found for {folder_name}")

# Create Notes
notes = []
def create_notes(batch, retry):
    url = "https://api.hubapi.com/crm/v3/objects/notes/batch/create"
    data = { "inputs": batch }
    try:
        response = requests.post(url, headers=hs_headers, json=data)
        response.raise_for_status()
        response_json = response.json()
        these_notes = response_json.get("results")
        log(f"Created Notes: {len(these_notes)}")
        notes.extend(these_notes)
    except requests.exceptions.RequestException as e:
        if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            create_notes(batch, retry)
        elif retry == 5:
            log("Max retries reached. Skipping batch.")
        else:
            error_msg = e.response.text if e.response is not None and e.response.text else str(e)
            log(f"Error creating Notes: {error_msg}")
for i in range(0, len(note_inputs), 100):
    batch = note_inputs[i:i+100]
    create_notes(batch, 0)
    time.sleep(0.25)
log(f"Total Notes created: {len(notes)}")

output_logs("migrate_files_log")