from datetime import datetime

import pytz


def datetime_from_timestamp(ts: int = None):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts).astimezone(pytz.utc)
