from dotenv import load_dotenv
import time
import os
import sys
from functions.logger import log, output_logs
from functions.parse_csv import parse_csv
from functions.search_records import SearchBody, search_records
from functions.associate_records import associate_records

load_dotenv()
PRIVATE_APP_KEY = os.getenv("PRIVATE_APP_KEY")
CONTACT_EXT_ID = os.getenv("CONTACT_EXT_ID")
COMPANY_EXT_ID = os.getenv("COMPANY_EXT_ID")
DEAL_EXT_ID = os.getenv("DEAL_EXT_ID")
DEAL_TO_COMPANY_PROP = os.getenv("DEAL_TO_COMPANY_PROP")
DEAL_TO_CONTACT_PROP = os.getenv("DEAL_TO_CONTACT_PROP")
if not PRIVATE_APP_KEY or not CONTACT_EXT_ID or not COMPANY_EXT_ID or not DEAL_EXT_ID or not DEAL_TO_COMPANY_PROP or not DEAL_TO_CONTACT_PROP:
    log("Error: Missing required environment variable(s).")
    sys.exit()

source_file_name = sys.argv[1] if len(sys.argv) > 1 else "deals.csv"
if not source_file_name:
    log("Error: Source file name not provided as argument.")
    sys.exit()

ext_deals: list[dict] = parse_csv(f"data/{source_file_name}")

# Get Deals from HubSpot
hs_deals = []
for i in range(0, len(ext_deals), 100):
    log(f"batch {i//100 + 1}/{len(ext_deals)//100 + 1}")
    batch = ext_deals[i:i+100]
    ext_deal_ids = list(set([record["Id"] for record in batch]))
    deal_search_body: SearchBody = {
        "filterGroups": [{
            "filters": [{
                "propertyName": DEAL_EXT_ID,
                "operator": "IN",
                "values": ext_deal_ids
            }]
        }],
        "limit": 100,
        "properties": [DEAL_EXT_ID]
    }
    these_deals = search_records("deals", deal_search_body, PRIVATE_APP_KEY)
    hs_deals.extend(these_deals)
    time.sleep(0.25)

# Add HubSpot Deal IDs to ext_deals
for ext_deal in ext_deals:
    for hs_deal in hs_deals:
        if hs_deal["properties"][DEAL_EXT_ID] == ext_deal["Id"]:
            ext_deal["hs_id"] = hs_deal["id"]
            break

# Get Companies from HubSpot
ext_deals_with_companies = [deal for deal in ext_deals if deal.get('AccountId')]
hs_companies = []
for i in range(0, len(ext_deals_with_companies), 100):
    log(f"batch {i//100 + 1}/{len(ext_deals_with_companies)//100 + 1}")
    batch = ext_deals_with_companies[i:i+100]
    ext_company_ids = list(set([deal[DEAL_TO_COMPANY_PROP] for deal in batch]))
    company_search_body: SearchBody = {
        "filterGroups": [{
            "filters": [{
                "propertyName": COMPANY_EXT_ID,
                "operator": "IN",
                "values": ext_company_ids
            }]
        }],
        "limit": 100,
        "properties": [COMPANY_EXT_ID]
    }
    these_companies = search_records("companies", company_search_body, PRIVATE_APP_KEY)
    hs_companies.extend(these_companies)
    time.sleep(0.25)

# Add HubSpot Company IDs to ext_deals
for ext_deal in ext_deals_with_companies:
    for company in hs_companies:
        if company["properties"][COMPANY_EXT_ID] == ext_deal[DEAL_TO_COMPANY_PROP]:
            ext_deal["company_hs_id"] = company["id"]
            break

# Associate Companies
company_associations = [deal for deal in ext_deals_with_companies if deal.get('hs_id') and deal.get('company_hs_id')]
associate_records(
   "deals",
   "hs_id",
   "companies",
   "company_hs_id",
   "HUBSPOT_DEFINED",
   5,
   company_associations,
   PRIVATE_APP_KEY
)

# Get Contacts from HubSpot
ext_deals_with_contacts = [deal for deal in ext_deals if deal.get('ContactId')]
hs_contacts = []
for i in range(0, len(ext_deals_with_contacts), 100):
    log(f"batch {i//100 + 1}/{len(ext_deals_with_contacts)//100 + 1}")
    batch = ext_deals_with_contacts[i:i+100]
    ext_contact_ids = list(set([deal[DEAL_TO_CONTACT_PROP] for deal in batch]))
    contact_search_body: SearchBody = {
        "filterGroups": [{
            "filters": [{
                "propertyName": CONTACT_EXT_ID,
                "operator": "IN",
                "values": ext_contact_ids
            }]
        }],
        "limit": 100,
        "properties": [CONTACT_EXT_ID]
    }
    these_contacts = search_records("contacts", contact_search_body, PRIVATE_APP_KEY)
    hs_contacts.extend(these_contacts)
    time.sleep(0.25)

# Add HubSpot Contact IDs to ext_deals
for deal in ext_deals_with_contacts:
    for contact in hs_contacts:
        if contact["properties"][CONTACT_EXT_ID] == deal[DEAL_TO_CONTACT_PROP]:
            deal["contact_hs_id"] = contact["id"]
            break

# Associate Contacts
contact_associations = [deal for deal in ext_deals_with_contacts if deal.get('hs_id') and deal.get('contact_hs_id')]
associate_records(
   "deals",
   "hs_id",
   "contacts",
   "contact_hs_id",
   "HUBSPOT_DEFINED",
   3,
   contact_associations,
   PRIVATE_APP_KEY
)

output_logs("assoc_deals_log")