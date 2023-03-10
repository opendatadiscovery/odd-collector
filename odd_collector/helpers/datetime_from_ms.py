from datetime import datetime
from typing import Optional

import pytz


def datetime_from_milliseconds(ms: Optional[int]) -> Optional[datetime]:
    """
    :param ms: milliseconds since the Unix epoch
    """
    if ms is None:
        return None

    return datetime.utcfromtimestamp(ms / 1000).astimezone(pytz.utc)
