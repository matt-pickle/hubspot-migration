import csv
from datetime import datetime
from pathlib import Path
from functions.logger import log

def write_to_csv(title, data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = Path(__file__).parent.parent.parent
    csv_filename = project_root / "logs" / f"{title}_{timestamp}.csv"
    fieldnames = data[0].keys() if data and len(data) > 0 else []
    if data and len(data) > 0:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
        log(f"Exported {len(data)} {title} to {csv_filename}")