from functions.logger import log, output_logs
from functions.parse_csv import parse_csv
from functions.search_records import SearchBody, search_records
from functions.batch_create_records import CreateInput, batch_create_records
from functions.write_to_csv import write_to_csv
from dotenv import load_dotenv
import time
import sys
import os

load_dotenv()
PRIVATE_APP_KEY = os.getenv("PRIVATE_APP_KEY")
NOTE_EXT_ID = os.getenv("NOTE_EXT_ID")
CONTACT_EXT_ID = os.getenv("CONTACT_EXT_ID")
COMPANY_EXT_ID = os.getenv("COMPANY_EXT_ID")
DEAL_EXT_ID = os.getenv("DEAL_EXT_ID")
if not PRIVATE_APP_KEY or not NOTE_EXT_ID or not CONTACT_EXT_ID or not COMPANY_EXT_ID or not DEAL_EXT_ID:
    log("Error: Missing required environment variable(s).")
    sys.exit()

source_file_name = sys.argv[1] if len(sys.argv) > 1 else "notes.csv"
if not source_file_name:
    log("Error: Source file name not provided as argument.")
    sys.exit()

notes: list[dict] = parse_csv(f"data/{source_file_name}")

# Get Contacts to associate
contact_ext_ids: list[str] = []
for note in notes:
    # Specific to Salesforce, modify as needed
    if note.get("ParentId") and note["ParentId"][:3] == "003":
        contact_ext_ids.append(note["ParentId"])
contact_ext_ids = list(set(contact_ext_ids))

contacts = []
for i in range(0, len(contact_ext_ids), 100):
    log(f"batch {i//100 + 1}/{len(contact_ext_ids)//100 + 1}")
    batch_contact_ext_ids = contact_ext_ids[i:i + 100]
    if len(batch_contact_ext_ids) == 0:
        continue
    contact_search_body: SearchBody = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": CONTACT_EXT_ID,
                        "operator": "IN",
                        "values": batch_contact_ext_ids
                    }
                ]
            }
        ],
        "properties": ["hs_object_id", CONTACT_EXT_ID],
        "limit": 100
    }
    contacts.extend(search_records("contacts", contact_search_body, PRIVATE_APP_KEY))
    time.sleep(0.25)

# Get Companies to associate
company_ext_ids: list[str] = []
for note in notes:
    # Specific to Salesforce, modify as needed
    if note.get("ParentId") and note["ParentId"][:3] == "001":
        company_ext_ids.append(note["ParentId"])
company_ext_ids = list(set(company_ext_ids))

companies = []
for i in range(0, len(company_ext_ids), 100):
    log(f"batch {i//100 + 1}/{len(company_ext_ids)//100 + 1}")
    batch_company_ext_ids = company_ext_ids[i:i + 100]
    if len(batch_company_ext_ids) == 0:
        continue
    company_search_body: SearchBody = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": COMPANY_EXT_ID,
                        "operator": "IN",
                        "values": batch_company_ext_ids
                    }
                ]
            }
        ],
        "properties": ["hs_object_id", COMPANY_EXT_ID],
        "limit": 100
    }
    companies.extend(search_records("companies", company_search_body, PRIVATE_APP_KEY))
    time.sleep(0.25)

# Get Deals to associate
deal_ext_ids: list[str] = []
for note in notes:
    # Specific to Salesforce, modify as needed
    if note.get("ParentId") and note["ParentId"][:3] == "006":
        deal_ext_ids.append(note["ParentId"])
deal_ext_ids = list(set(deal_ext_ids))

deals = []
for i in range(0, len(deal_ext_ids), 100):
    log(f"batch {i//100 + 1}/{len(deal_ext_ids)//100 + 1}")
    batch_deal_ext_ids = deal_ext_ids[i:i + 100]
    if len(batch_deal_ext_ids) == 0:
        continue
    deal_search_body: SearchBody = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": DEAL_EXT_ID,
                        "operator": "IN",
                        "values": batch_deal_ext_ids
                    }
                ]
            }
        ],
        "properties": ["hs_object_id", DEAL_EXT_ID],
        "limit": 100
    }
    deals.extend(search_records("deals", deal_search_body, PRIVATE_APP_KEY))
    time.sleep(0.25)

# Create Notes
inputs: list[CreateInput] = []
for note in notes:

    # Specific to Salesforce, modify as needed
    properties = {
        NOTE_EXT_ID: note["Id"],
        "hs_note_body": note.get("Body") or note.get("CommentBody") or "",
        "hs_timestamp": note["CreatedDate"].replace("+0000", "Z"),
    }

    associations = []
    # Add Contact association
    if note.get("ParentId") and note["ParentId"][:3] == "003": # Specific to Salesforce, modify as needed
        contact_ids: list[str] = [contact["id"] for contact in contacts if contact["properties"].get(CONTACT_EXT_ID) == note["ParentId"]]
        if len(contact_ids):
            associations.append({
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 202
                    }
                ],
                "to": {
                    "id": contact_ids[0]
                }
            })

    # Add Company association
    if note.get("ParentId") and note["ParentId"][:3] == "001": # Specific to Salesforce, modify as needed
        company_ids: list[str] = [company["id"] for company in companies if company["properties"].get(COMPANY_EXT_ID) == note["ParentId"]]
        if len(company_ids):
            associations.append({
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 190
                    }
                ],
                "to": {
                    "id": company_ids[0]
                }
            })

    # Add Deal association
    if note.get("ParentId") and note["ParentId"][:3] == "006": # Specific to Salesforce, modify as needed
        deal_ids: list[str] = [deal["id"] for deal in deals if deal["properties"].get(DEAL_EXT_ID) == note["ParentId"]]
        if len(deal_ids):
            associations.append({
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 214
                    }
                ],
                "to": {
                    "id": deal_ids[0]
                }
            })

    if len(associations):
        input: CreateInput = {
            "properties": properties,
            "associations": associations
        }
        inputs.append(input)

log(f"Total notes to create: {len(inputs)}")

batch_create_records("notes", inputs, PRIVATE_APP_KEY)

# Write non-imported notes to CSV
non_imported_notes = [note for note in notes if note["Id"] not in [input["properties"][NOTE_EXT_ID] for input in inputs]]
write_to_csv("logs/non_imported_notes", non_imported_notes)

output_logs("migrate_notes_log")