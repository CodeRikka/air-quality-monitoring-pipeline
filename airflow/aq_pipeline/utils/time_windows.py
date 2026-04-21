from datetime import datetime, timedelta, timezone


def hot_window_bounds(hours: int = 48) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = now - timedelta(hours=1)
    start_utc = end_utc - timedelta(hours=hours - 1)
    return start_utc, end_utc
