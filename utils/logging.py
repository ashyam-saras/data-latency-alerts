import json
from datetime import datetime

def cprint(message: str, severity: str = "INFO", **kwargs):
    """Cloud logging wrapper with timestamp, search term, and unique ID"""
    timestamp = datetime.now().isoformat()
    entry = {
        "timestamp": timestamp,
        "severity": severity,
        "message": message,
        **kwargs,
    }
    print(json.dumps(entry))