from typing import TypedDict, NotRequired, Literal, Any
import requests
import time
from functions.logger import log

class Record(TypedDict):
    id: str
    properties: dict[str, Any]

class Filter(TypedDict):
    propertyName: str
    operator: Literal["IN"] | Literal["NOT_HAS_PROPERTY"] | Literal["LT"] | Literal["EQ"] | Literal["GT"] | Literal["NOT_IN"] | Literal["GTE"] | Literal["CONTAINS_TOKEN"] | Literal["HAS_PROPERTY"] | Literal["LTE"] | Literal["NOT_CONTAINS_TOKEN"] | Literal["BETWEEN"] | Literal["NEQ"]
    values: NotRequired[list[str]]
    value: NotRequired[str]

class SearchBody(TypedDict):
    filterGroups: list[dict[Literal["filters"], list[Filter]]]
    properties: list[str]
    limit: int
    after: NotRequired[str | int]
    sorts: NotRequired[list[dict[str, str]]]

class Response(TypedDict):
    results: list[Record]
    total: int
    paging: NotRequired[dict[Literal["next"], dict[Literal["after"], str]]]

def search_records(
    record_type: str,
    search_body: SearchBody,
    PRIVATE_APP_KEY: str,
    records: list[Record] | None = None,
    retry: int = 0
) -> list[Record]:
    if records is None:
        records = []
    url = f"https://api.hubapi.com/crm/v3/objects/{record_type}/search"
    headers = { "Authorization": f"Bearer {PRIVATE_APP_KEY}", "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json=search_body)
        response.raise_for_status()
        json_response = response.json()
        records.extend(json_response["results"])
        log(f"Retrieved {record_type}: {len(json_response['results'])}")
        paging = json_response.get("paging")
        if paging:
            next = paging.get("next")
            if next:
                new_after = next.get("after")
                if new_after:
                    search_body["after"] = new_after
                    search_records(record_type, search_body, PRIVATE_APP_KEY, records, 0)
    except requests.exceptions.RequestException as e:
        if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            search_records(record_type, search_body, PRIVATE_APP_KEY, records, retry)
        elif retry == 5:
            log("Max retries reached. Skipping batch.")
        else:
            error_msg = e.response.text if e.response is not None and e.response.text else str(e)
            log(f"Error retrieving {record_type}: {error_msg}")    

    return records