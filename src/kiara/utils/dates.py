# -*- coding: utf-8 -*-
import time
from datetime import datetime

import humanfriendly
import pytz


def get_current_time_incl_timezone() -> datetime:

    current_tz_name = time.tzname[0]
    current_tz = pytz.timezone(current_tz_name)

    return datetime.now(tz=current_tz)


def get_earliest_time_incl_timezone() -> datetime:
    return datetime(1970, 1, 1, tzinfo=pytz.utc)


def to_human_readable_date_string(datetime: datetime) -> str:

    now = get_current_time_incl_timezone()
    time_gone = (now - datetime).total_seconds()

    relative_time_str: str = humanfriendly.format_timespan(time_gone, max_units=1)
    return relative_time_str
