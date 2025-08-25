import datetime as dt

from typing import Optional


def date_to_str (date : str | dt.datetime = None) -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if date is None:
        date = dt.datetime.now()
    
    return date.strftime("%Y-%m-%d") if isinstance(date, dt.datetime) else str(date)


def time_to_str (time : str | dt.time = None) -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if time is None :
        time = dt.datetime.now().time()

    return time.strftime("%H:%M:%S") if isinstance(time, dt.time) else str(time)


def check_date_format (date_str: str) -> Optional[dt.datetime] :
    """
    Validate `YYYY-MM-DD` and return a datetime or None.
    """
    try :

        return dt.datetime.strptime(date_str, "%Y-%m-%d")
    
    except Exception :
        
        return None