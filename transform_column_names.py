import os
from dotenv import load_dotenv
import sys
from functions.logger import output_logs
from functions.parse_csv import parse_csv
from functions.write_to_csv import write_to_csv

load_dotenv()
SOURCE_COLUMN_NAME = os.getenv("SOURCE_COLUMN_NAME")
HS_COLUMN_NAME = os.getenv("HS_COLUMN_NAME")
if not SOURCE_COLUMN_NAME or not HS_COLUMN_NAME:
    print("Error: Missing required environment variable(s).")
    sys.exit()

object_name = sys.argv[1] if len(sys.argv) > 1 else None
if not object_name:
    print("Error: Object name not provided as argument.")
    sys.exit()
    
mapping = parse_csv(f"mapping/{object_name}")
data = parse_csv(f"data/{object_name}")

for row in data:
    if SOURCE_COLUMN_NAME in row:
        row[HS_COLUMN_NAME] = row.pop(SOURCE_COLUMN_NAME)

write_to_csv(f"transformed/{object_name}", data)

output_logs("transformation_log")