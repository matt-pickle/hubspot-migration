from datetime import datetime
from pathlib import Path

logs = ""

def log(message):
    global logs
    print(message)
    logs = f"{logs}\n{message}"

def output_logs(file_name):
    timestamp = datetime.now().isoformat()
    log_file_name = f"{file_name}_{timestamp}.txt"
    log_dir = Path(__file__).parent.parent.parent / "logs" / log_file_name
    with open(log_dir, "w") as f:
        f.write(logs)
    print(f"Log saved to {log_file_name}")
