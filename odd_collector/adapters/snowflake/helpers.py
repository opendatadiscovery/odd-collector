from datetime import datetime
from typing import Optional

import pytz


def transform_datetime(table_time: datetime) -> Optional[str]:
    if table_time:
        table_time.astimezone(pytz.utc).isoformat()
    return None
