import datetime as dt

from typing import Optional


def date_to_str (date : Optional[str | dt.datetime] = None, format : str = "%Y-%m-%d") -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if date is None:
        date_obj = dt.datetime.now()

    elif isinstance(date, dt.datetime):
        date_obj = date

    elif isinstance(date, dt.date):  # handles plain date (without time)
        date_obj = dt.datetime.combine(date, dt.time.min) # This will add 00 for the time

    elif isinstance(date, str) :

        try:
            date_obj = dt.datetime.strptime(date, format)

        except ValueError :
            
            try :
                date_obj = dt.datetime.fromisoformat(date)
            
            except ValueError :
                raise ValueError(f"Unrecognized date format: '{date}'")
    
    else :
        raise TypeError("date must be a string, datetime, or None")

    return date_obj.strftime(format)


def time_to_str (time : Optional[str | dt.time] = None, format : str = "%H:%M:%S") -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "HH:MM" format.
    """
    if time is None :
        time = dt.datetime.now().time()

    return time.strftime(format) if isinstance(time, dt.time) else str(time)


def check_date_format (date_str: str, format : str = "%Y-%m-%d") -> Optional[dt.datetime] :
    """
    Validate `YYYY-MM-DD` and return a datetime or None.
    """
    try :

        return dt.datetime.strptime(date_str, "%Y-%m-%d")
    
    except Exception :
        
        return None