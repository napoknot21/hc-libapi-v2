import os
import csv
import datetime as dt
import polars as pl

from typing import Optional, Tuple, Dict, List

from libapi.config.parameters import (
    LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME, LIBAPI_LOGS_REQUESTS_COLUMNS
)
from libapi.utils.formatter import date_to_str

def write_to_file (
    
        id : Optional[str | int] = None,

        date : Optional[str | dt.datetime | dt.date] = None,
        fund : Optional[str] = None,
        type : Optional[str] = None,

        format : str = "%Y-%m-%d %H:%M:%S",
        file_abs_path : Optional[str] = None,
    
    ) -> None :
    """
    
    """
    if id is None :

        print(f"\n[-] Not ID is specified. No caculation added")
        return

    date = date_to_str(date)
    fund = "HV" if fund is None else fund
    type = "IM" if type is None else type

    CALCULATIONS_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME)
    file_abs_path = CALCULATIONS_ABS_PATH if file_abs_path is None else file_abs_path
    
    obj_date = dt.datetime.strptime(date, format[:8]) # We only use the "%Y-%m-%s" (lenght = 8)
    
    # Construct the new row
    new_row = {
    
        "Date": obj_date.strftime(format),
        "ID": int(id),
        "Type": type,
        "Fundation": fund
    
    }

    file_exists = os.path.isfile(file_abs_path)
    
    if file_exists == True :

        # Check if the date or ID already exists in the file
        if has_duplicates(id, date, fund, type, file_abs_path=file_abs_path) :
            
            print("\n[-] Error: Date or ID already exists in the file.")
            return

    with open(file_abs_path, mode="a", newline="", encoding="utf-8") as f :
        
        writer = csv.DictWriter(f, fieldnames=new_row.keys())
        
        if not file_exists :
            writer.writeheader()

        writer.writerow(new_row)

    print(f"\n[+] New row added: {new_row}")


def has_duplicates (

        id : str | int,

        date : Optional[str | dt.datetime | dt.date] = None,
        fund : Optional[str] = None,
        type : Optional[str] = None,

        format : str = "%Y-%m-%d",
        
        schema_override : Optional[Dict] = None,
        specific_columns : Optional[List] = None,
        
        file_abs_path : Optional[str] = None
    
    ) -> bool :
    """
    Check whether a record already exists in the calculations log file.

    Args:
        id: Unique calculation ID (string or integer).
        date: Run date (string or datetime).
        type: Calculation type.
        fund: Fund identifier (default "HV").
        schema_override: Optional dictionary of Polars dtypes for columns.
        specific_columns: Columns to read from CSV (defaults to schema_override keys).
        file_abs_path: Absolute path of the log CSV (defaults to config path).

    Returns:
        True if a duplicate entry exists, False otherwise.
    """
    date = date_to_str(date, format)
    fund = "HV" if fund is None else fund

    CALCULATIONS_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME)
    file_abs_path = CALCULATIONS_ABS_PATH if file_abs_path is None else file_abs_path
    
    schema_override = LIBAPI_LOGS_REQUESTS_COLUMNS if schema_override is None else schema_override
    specific_columns = list(schema_override.keys()) if specific_columns is None else specific_columns

    # Dataframe
    df = pl.read_csv(file_abs_path, schema_overrides=schema_override, columns=specific_columns)
    
    print(df)

    df_id = df.filter((pl.col("ID") == int(id)))

    if df_id.is_empty() == True :
        return False

    date_obj = dt.datetime.strptime(date, format)
    filtered = df.filter((pl.col("Date") == pl.lit(date_obj)))

    results = filtered.filter((pl.col("Type") == type) & (pl.col("Fundation") == fund))

    # If no matching date, ID, or calculation type is found, return False
    return results.is_empty()


def read_id_from_file (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fund : Optional[str] = None,
        type : Optional[str] = None,

        time_sensitive : bool = False,

        format : str = "%Y-%m-%d",
        schema_override : Optional[Dict] = None,
        file_abs_path : Optional[str] = None,

    ) :
    """
    Returns the ID of calculationtype and fund
    """
    date = date_to_str(date)
    fund = "HV" if fund is None else fund
    type = "IM" if type is None else type

    CALCULATIONS_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME)
    file_abs_path = CALCULATIONS_ABS_PATH if file_abs_path is None else file_abs_path

    schema_override = LIBAPI_LOGS_REQUESTS_COLUMNS if schema_override is None else schema_override
    specific_cols = list(schema_override.keys())

    dataframe = pl.read_csv(file_abs_path, schema_overrides=schema_override, columns=specific_cols)

    print(dataframe)
    
    # First filter
    filtered = dataframe.filter((pl.col("Type") == type) & (pl.col("Fundation") == fund))

    if filtered.is_empty() : return None

    if time_sensitive == True :
        
        date_obj = dt.datetime.strptime(date, format)
        result = filtered.filter((pl.col("Date") == pl.lit(date_obj)))

    else :

        # Match by date only (ignore time component)
        result = filtered.filter((pl.col("Date").dt.strftime(format) == date))

    # Return the ID if found
    if result.height > 0 :

        return result[0, "ID"]

    # If no match found
    return None


def get_most_recent_calculation (
        
        type : str = "IM",
        fund : Optional[str] = None,
        format : str = "%Y-%m-%d",
        schema_overrides : Optional[Dict] = None,
        specific_cols : Optional[List] = None,
        file_abs_path : Optional[str] = None,
        
    ) -> Tuple[Optional[str], Optional[int]] :
    """
    Read the calculations log CSV with Polars and return the latest run time and ID
    for the given calculation_type and fund.

    CSV schema (with headers):
      - Date: pl.Date (or pl.Datetime if present)
      - ID: pl.Utf8 (or Int; coerced to string for safety)
      - Type: pl.Utf8
      - Fund: pl.Utf8   (legacy: 'Fundation' supported and renamed to 'Fund')

    Args:


    Returns:
      (last_run_time_formatted: str | None, last_run_id: str | None)
    """
    last_run_date = None
    last_run_id = None

    fund = "HV" if fund is None else fund

    CALCULATIONS_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME)
    file_abs_path = CALCULATIONS_ABS_PATH if file_abs_path is None else file_abs_path

    schema_overrides = LIBAPI_LOGS_REQUESTS_COLUMNS if schema_overrides is None else schema_overrides
    specific_cols = list(schema_overrides.keys()) if specific_cols is None else specific_cols

    try :
        df = pl.read_csv(file_abs_path, schema_overrides=schema_overrides, columns=specific_cols)

    except Exception as e :
        
        print(f"\n[-] Error during reading CSV file. {e}")
        return None, None
    
    filtered = df.filter((pl.col("Type") == type) & (pl.col("Fundation") == fund))

    if filtered.height == 0:
        return None, None
    
    # Pick the row with the max Date; if ties, keep the last occurrence
    last_row = filtered.sort("Date").tail(1)

    last_run_date = last_row[0, "Date"].strftime(format)
    last_run_id = last_row[0, "ID"]

    # Return the last run time as a datetime object
    return last_run_date, last_run_id


def get_closest_date_calculation_by_type (
        
        date : Optional[str | dt.datetime | dt.date], # Target Date
        type : Optional[str],
        fund : Optional[str] = None,
        format : str = "%Y-%m-%d",
        schema_overrides : Optional[Dict] = None,
        specific_cols : Optional[List] = None,
        file_abs_path : Optional[str] = None,

    ) -> Tuple[Optional[str], Optional[int]] : 
    """
    
    """
    date = date_to_str(date)
    type = "MV" if type is None else type
    fund = "HV" if fund is None else fund

    CALCULATIONS_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_CALCULATIONS_BASENAME)
    file_abs_path = CALCULATIONS_ABS_PATH if file_abs_path is None else file_abs_path

    schema_overrides = LIBAPI_LOGS_REQUESTS_COLUMNS if schema_overrides is None else schema_overrides
    specific_cols = list(schema_overrides.keys()) if specific_cols is None else specific_cols

    df = pl.read_csv(file_abs_path, schema_overrides=schema_overrides, columns=specific_cols)

    # Filter by type and fund if provided
    df = df.filter(pl.col("Type") == type)

    # If some files still have Fundation, the rename above covers it
    df = df.filter(pl.col("Fundation") == fund)

    if df.is_empty() :
        return None, None

    target_dt = dt.datetime.strptime(date, format)

    # tie-breaker by Date
    df = df.with_columns((pl.col("Date") - pl.lit(target_dt)).abs().alias("abs_diff")).sort(["abs_diff", "Date"])  

    row = df.row(0, named=True)

    closest_date = row["Date"].strftime(format)
    closest_id = int(row["ID"])

    return closest_date, closest_id