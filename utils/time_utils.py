# utils/time_utils.py
from datetime import datetime, timedelta, timezone
import re

# يمكنك تعديل هذه الدالة لو أردت استخدام توقيت عالمي (UTC) بدلاً من المحلي
# NOW_FUNC = datetime.utcnow
NOW_FUNC = datetime.now

def parse_relative_time(time_str: str | None) -> datetime:
    """
    Parses a relative time string or specific timestamp into a datetime object.

    Handles:
    - "now"
    - "-<number>h" (hours ago)
    - "-<number>d" (days ago)
    - "-<number>m" (minutes ago)
    - "-<number>s" (seconds ago)
    - "-<number> hour" or "-<number> hours" (hours ago)
    - "-<number> day" or "-<number> days" (days ago)
    - "-<number> minute" or "-<number> minutes" (minutes ago)
    - "-<number> second" or "-<number> seconds" (seconds ago)
    - ISO 8601 format timestamps (e.g., "2023-10-27T10:00:00Z")

    Args:
        time_str: The time string to parse. If None, returns current time.

    Returns:
        A datetime object.

    Raises:
        ValueError: If the format is unrecognized.
    """
    if time_str is None:
        return NOW_FUNC()

    time_str = time_str.lower().strip()

    if time_str == "now":
        return NOW_FUNC()

    # Check for relative time format with units (e.g., -8h)
    match = re.match(r"^-(\d+)([hmsd])$", time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        now = NOW_FUNC()

        if unit == 'h':
            return now - timedelta(hours=value)
        elif unit == 'm':
            return now - timedelta(minutes=value)
        elif unit == 's':
            return now - timedelta(seconds=value)
        elif unit == 'd':
            return now - timedelta(days=value)

    # Check for relative time format with words (e.g., -1 hour, -5 days)
    match = re.match(r"^-(\d+)\s+(hour|hours|day|days|minute|minutes|second|seconds)$", time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        now = NOW_FUNC()

        if unit in ('hour', 'hours'):
            return now - timedelta(hours=value)
        elif unit in ('day', 'days'):
            return now - timedelta(days=value)
        elif unit in ('minute', 'minutes'):
            return now - timedelta(minutes=value)
        elif unit in ('second', 'seconds'):
            return now - timedelta(seconds=value)

    # Check for ISO 8601 format (add more formats if needed)
    try:
        # Attempt to parse common ISO formats
        # Handle 'Z' for UTC timezone explicitly
        if time_str.endswith('z'):
            return datetime.fromisoformat(time_str[:-1]).replace(tzinfo=timezone.utc)
        else:
            # Let fromisoformat handle timezone offset like +02:00 if present
            return datetime.fromisoformat(time_str)
    except ValueError:
        # If ISO parsing fails, raise error about unrecognized format
        raise ValueError(f"Invalid or unrecognized time format: '{time_str}'. Use 'now', '-<num>[h|m|s|d]', '-<num> hour(s)/day(s)/etc', or ISO 8601 format.")

# Example Usage (optional, for testing)
if __name__ == '__main__':
    print(f"Now: {parse_relative_time('now')}")
    print(f"-1h: {parse_relative_time('-1h')}")
    print(f"-30m: {parse_relative_time('-30m')}")
    print(f"-2d: {parse_relative_time('-2d')}")
    print(f"-1 hour: {parse_relative_time('-1 hour')}")
    print(f"-5 days: {parse_relative_time('-5 days')}")
    print(f"ISO UTC: {parse_relative_time('2023-10-26T12:00:00Z')}")
    print(f"ISO Offset: {parse_relative_time('2023-10-26T14:00:00+02:00')}")
    try:
        parse_relative_time("invalid")
    except ValueError as e:
        print(f"Error (expected): {e}")