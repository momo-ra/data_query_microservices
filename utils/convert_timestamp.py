from datetime import datetime

def convert_timestamp_format(timestamp: str):
    """Convert multiple date formats to PostgreSQL standard format."""
    accepted_formats = [
        "%Y-%m-%d %H:%M:%S",  # 2024-11-29 08:00:00
        "%Y-%m-%d",  # 2024-11-29
        "%d/%m/%Y %H:%M:%S",  # 29/11/2024 08:00:00
        "%d/%m/%Y",  # 29/11/2024
        "%m/%d/%Y %H:%M:%S",  # 11/29/2024 08:00:00
        "%m/%d/%Y",  # 11/29/2024
    ]

    for fmt in accepted_formats:
        try:
            return datetime.strptime(timestamp, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {timestamp}")