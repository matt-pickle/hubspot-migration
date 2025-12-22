from typing import TypedDict, Literal, NotRequired
import requests
import time
from functions.logger import log

class AssociationType(TypedDict):
    associationCategory: Literal["HUBSPOT_DEFINED", "USER_DEFINED"]
    associationTypeId: int

class Association(TypedDict):
    types: list[AssociationType]
    to: dict[Literal["id"], str]

class CreateInput(TypedDict):
    properties: dict[str, str]
    associations: NotRequired[list[Association]]

def batch_create_records(
    record_type: str,
    inputs: list[CreateInput],
    PRIVATE_APP_KEY: str,
) -> list[dict]:
    url = f"https://api.hubapi.com/crm/v3/objects/{record_type}/batch/create"
    headers = { "Authorization": f"Bearer {PRIVATE_APP_KEY}", "Content-Type": "application/json" }
    records: list[dict] = []

    def create_batch(batch: list[CreateInput], retry: int):
        data = { "inputs": batch }
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            json_response = response.json()
            records.extend(json_response["results"])
            log(f"Created {record_type}: {len(json_response['results'])}")
        except requests.exceptions.RequestException as e:
            if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
                retry = retry + 1
                interval = retry * 2
                log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
                time.sleep(interval)
                create_batch(batch, retry)
            elif retry == 5:
                log("Max retries reached. Skipping batch.")
            else:
                error_msg = e.response.text if e.response is not None and e.response.text else str(e)
                log(f"Error creating {record_type}: {error_msg}")

    for i in range(0, len(inputs), 100):
        log(f"batch {i//100 + 1}/{len(inputs)//100 + 1}")
        create_batch(inputs[i:i+100], 0)
        time.sleep(0.25)
    
    log(f"Total {record_type} created: {len(records)}")
    return records