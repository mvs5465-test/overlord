from __future__ import annotations

from datetime import datetime, timezone


def format_relative_time(value: datetime) -> str:
    now = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta_seconds = int((now - value.astimezone(timezone.utc)).total_seconds())
    if delta_seconds < 60:
        return "just now"
    if delta_seconds < 3600:
        minutes = delta_seconds // 60
        return f"{minutes}m ago"
    if delta_seconds < 86400:
        hours = delta_seconds // 3600
        return f"{hours}h ago"
    days = delta_seconds // 86400
    return f"{days}d ago"
