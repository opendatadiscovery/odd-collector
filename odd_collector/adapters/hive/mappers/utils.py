from datetime import datetime

import pytz


def transform_datetime(table_time: datetime):
    if table_time is None:
        return None
    return table_time.astimezone(pytz.utc).isoformat()
