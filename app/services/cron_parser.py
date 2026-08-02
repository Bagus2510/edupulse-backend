"""Cron expression utilities for pipeline scheduling."""
from datetime import datetime, timedelta


CRON_DESCRIPTIONS = {
    "@hourly": "Setiap jam",
    "@daily": "Setiap hari",
    "@weekly": "Setiap minggu",
    "@monthly": "Setiap bulan",
    "@yearly": "Setiap tahun",
    "manual": "Manual",
    "": "Manual",
}


def cron_to_human(expression: str) -> str:
    """Convert cron expression to Bahasa Indonesia description."""
    if not expression or expression in ("manual", "None", "none"):
        return "Manual"

    clean = expression.strip()
    if clean in CRON_DESCRIPTIONS:
        return CRON_DESCRIPTIONS[clean]

    parts = clean.split()
    if len(parts) != 5:
        return clean

    minute, hour, day, month, dow = parts

    # Build description
    dow_map = {"1-5": "hari kerja", "0,6": "akhir pekan", "*": ""}
    dow_desc = dow_map.get(dow, "")

    if hour != "*" and minute != "*":
        time_str = f"jam {hour.zfill(2)}:{minute.zfill(2)}"
    elif hour != "*":
        time_str = f"jam {hour.zfill(2)}:00"
    else:
        time_str = "setiap saat"

    if day == "*" and month == "*":
        if dow_desc:
            return f"Setiap {dow_desc} {time_str}".strip()
        return f"Setiap hari {time_str}".strip()
    elif day != "*" and month == "*":
        return f"Tanggal {day} setiap bulan {time_str}".strip()
    elif day == "*" and month != "*":
        return f"Setiap hari di bulan {month} {time_str}".strip()

    return clean


def next_run_time(expression: str, after: datetime | None = None) -> datetime | None:
    """Calculate next run time from cron expression. Returns None for manual."""
    if not expression or expression in ("manual", "None", "none"):
        return None

    clean = expression.strip()
    now = after or datetime.now()

    if clean == "@hourly":
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return next_hour

    if clean == "@daily":
        next_day = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return next_day

    if clean == "@weekly":
        days_ahead = 7 - now.weekday()
        if days_ahead == 0:
            days_ahead = 7
        return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)

    if clean == "@monthly":
        if now.month == 12:
            return datetime(now.year + 1, 1, 1)
        return datetime(now.year, now.month + 1, 1)

    # Try to parse simple HH:MM daily patterns
    parts = clean.split()
    if len(parts) == 5:
        minute, hour = parts[0], parts[1]
        if hour != "*" and minute != "*":
            try:
                h, m = int(hour), int(minute)
                candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if candidate <= now:
                    candidate += timedelta(days=1)
                return candidate
            except ValueError:
                pass

    return None
