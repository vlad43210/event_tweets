import pytz
from datetime import datetime, timedelta

def is_dst(zonename, dt_str, dt_format):
    tz = pytz.timezone(zonename)
    now = pytz.utc.localize(datetime.strptime(dt_str, dt_format))
    return now.astimezone(tz).dst() != timedelta(0)
