from datetime import datetime
from typing import Optional

import pytz


class Datetime:
    TIMEZONE = pytz.UTC

    def __init__(self, dt: Optional[datetime]) -> None:
        self.datetime = None if dt is None else dt.replace(tzinfo=self.TIMEZONE)

    @classmethod
    def from_ms(cls, ms: Optional[int]) -> "Datetime":
        """
        :param ms: milliseconds since the Unix epoch
        """
        return cls(None) if ms is None else cls(datetime.utcfromtimestamp(ms / 1000))

    @classmethod
    def from_timestamp(cls, timestamp: Optional[int]) -> "Datetime":
        return (
            cls(None) if timestamp is None else cls(datetime.fromtimestamp(timestamp))
        )
