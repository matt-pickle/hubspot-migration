import requests
import time
from functions.logger import log

def associate_records(from_record_type, from_id_property, to_record_type, to_id_property, association_category, association_type_id, associations, PRIVATE_APP_KEY):
   url = f"https://api.hubapi.com/crm/v4/associations/{from_record_type}/{to_record_type}/batch/create"
   headers = { "Authorization": f"Bearer {PRIVATE_APP_KEY}", "Content-Type": "application/json" }
   def associate_batch(batch, retry):
      inputs = []
      for assoc in batch:
         input = {
            "types": [{
               "associationCategory": association_category,
               "associationTypeId": association_type_id
            }],
            "from": { "id": assoc[from_id_property] },
            "to": { "id": assoc[to_id_property] }
         }
         inputs.append(input)
      data = { "inputs": inputs }
      try:
         response = requests.post(url, headers=headers, json=data)
         response.raise_for_status()
         json_response = response.json()
         log(f"Associated {from_record_type} to {to_record_type}: {len(json_response['results'])}")
      except requests.exceptions.RequestException as e:
         if e.response is not None and (e.response.status_code == 429 or str(e.response.status_code)[0] == "5") and retry < 5:
            retry = retry + 1
            interval = retry * 2
            log(f"{'Rate limit exceeded' if e.response.status_code == 429 else 'Server error'}. Retrying after {interval} seconds...")
            time.sleep(interval)
            associate_batch(batch, retry)
         elif retry == 5:
               log("Max retries reached. Skipping batch.")
         else:
               error_msg = e.response.text if e.response is not None and e.response.text else str(e)
               log(f"Error associating {from_record_type} to {to_record_type}: {error_msg}")

   for i in range(0, len(associations), 500):
      log(f"batch {i//500 + 1}/{len(associations)//500 + 1}")
      batch = associations[i:i+500]
      associate_batch(batch, 0)
      time.sleep(0.25)

