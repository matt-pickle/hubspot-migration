import csv
import os
from pathlib import Path
from functions.logger import log

def parse_csv(csv_file_path):
    # Get absolute path if relative path is provided
    if not os.path.isabs(csv_file_path):
        project_root = Path(__file__).parent.parent.parent
        csv_file_path = project_root / csv_file_path
    
    # Check if file exists
    if not os.path.exists(csv_file_path):
        log(f"Error: File not found at {csv_file_path}")
        return []
    
    # Parse CSV to list of dictionaries
    data = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                data.append(row)
        
        log(f"Successfully parsed {len(data)} rows from {csv_file_path}")
        return data
    except Exception as e:
        log(f"Error parsing CSV file: {str(e)}")
        return []