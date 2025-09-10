import os
import csv
import datetime as dt
import polars as pl

from typing import Optional, Tuple

from libapi.config.parameters import CALCULATION_ID_FILE_ABS_PATH, CALCULATION_ID_CSV_ABS_PATH
from libapi.utils.formatter import date_to_str

def write_to_file (
    
        id : str | int,
        date : str | dt.datetime,
        calculation_type : str,
        fund : Optional[str] = None,
        file_abs_path : Optional[str] = None,
        format_date : str = "%Y-%m-%d %H:%M:%S"
    
    ) -> None :
    """
    
    """
    if not id :
        return

    fund = "HV" if fund is None else fund
    file_abs_path = CALCULATION_ID_CSV_ABS_PATH if file_abs_path is None else file_abs_path

    # Convert date to string if it's a datetime object
    if isinstance(date, dt.datetime):
        date_str = date.strftime(format)
    else:
        date_str = date
    
    # Get the current date if not provided
    verified_date = date_to_str(date, format=format_date)

    # Construct the new row
    new_row = {
    
        "Date": dt.datetime.strptime(date_str, format),
        "ID": int(id),
        "Type": calculation_type,
        "Fundation": fund
    
    }
        
    # Check if the date or ID already exists in the file
    if check_duplicate(date, id, fund, file_abs_path) :
        raise ValueError("Error: Date or ID already exists in the file.")

    # Create a DataFrame with the new row
    new_row_df = pl.DataFrame([new_row])

    # Define schema to ensure consistency
    schema_override = {
        "Date": pl.Datetime,
        "ID": pl.Int128,
        "Type": pl.Utf8,
        "Fundation": pl.Utf8
    }

    # Check if the file exists
    try:
        # Try to read the file to check if it exists
        existing_df = pl.read_csv(file_abs_path, schema_overrides=schema_override)
        # If the file exists, append the new row
        updated_df = existing_df.vstack(new_row_df)
    except FileNotFoundError:
        # If the file doesn't exist, just use the new row DataFrame
        updated_df = new_row_df

    # Write the updated DataFrame to the file
    updated_df.write_csv(file_abs_path)

    print(f"New row added: {new_row}")


def write_file_to_csv (file_abs_path : str = CALCULATION_ID_FILE_ABS_PATH, csv_abs_path : str = CALCULATION_ID_CSV_ABS_PATH) :

    # Check if the output CSV exists
    if not os.path.exists(CALCULATION_ID_CSV_ABS_PATH):
        # Create the CSV file with headers
        print("[*] Creating new CSV file with headers...")
        empty_df = pl.DataFrame([
            pl.Series(name="Date", values=[], dtype=pl.Utf8),
            pl.Series(name="ID", values=[], dtype=pl.Int64),
            pl.Series(name="Type", values=[], dtype=pl.Utf8),
            pl.Series(name="Fundation", values=[], dtype=pl.Utf8),
        ])
        empty_df.write_csv(csv_abs_path)

    # Prepare lists to store parsed lines
    date_list = []
    id_list = []
    type_list = []
    fund_list = []

    # Read and parse the source file

    with open(file_abs_path, 'r') as file:
        for line in file:
            date_str, id_str, type_str, fund_str = split_line(line)

            # Optionally convert types
            id_int = int(id_str)  # Convert ID to int
            date_cleaned = date_str.strip()

            # Append to lists
            date_list.append(date_cleaned)
            id_list.append(id_int)
            type_list.append(type_str)
            fund_list.append(fund_str)

    # Create a Polars DataFrame
    df = pl.DataFrame({
        "Date": date_list,
        "ID": id_list,
        "Type": type_list,
        "Fundation": fund_list,
    })

    # 1. Parse date strings into actual datetime objects
    df = df.with_columns([
        pl.col("Date").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")
    ])

    # 2. Sort using datetime column
    df = df.sort("Date")

    # 3. Format the Date column back to string AFTER sorting
    df = df.with_columns([
        pl.col("Date").dt.strftime("%Y-%m-%d %H:%M:%S").alias("Date")
    ])

    # Append to the existing CSV file
    df.write_csv(CALCULATION_ID_CSV_ABS_PATH)
    print(f"[*] Successfully appended {len(df)} rows to CSV.")


def check_duplicate (

        id : str | int,
        date : str | dt.datetime,
        calculation_type : str,
        fund : Optional[str] = None,
        file_abs_path : Optional[str] = None
    
    ) -> bool :
    """
    
    """
    fund = "HV" if fund is None else fund
    file_abs_path = CALCULATION_ID_FILE_ABS_PATH if file_abs_path is None else file_abs_path

    # Open the file in read mode
    with open(file_abs_path, 'r') as file :

        # Iterate through each line in the file
        for line in file :
            
            date_line, id_line, type_line, fund_line = split_line(line)
            
            # Check if the date, ID, and calculation type already exist
            if date_line == date and id_line == id and type_line == calculation_type and fund_line == fund :
                # Date, ID, and calculation type already exist
                return True
            
    # If no matching date, ID, or calculation type is found, return False
    return False


def split_line (line : str) -> Tuple[str, str, str, str]:
    """
    
    """
    # Split the input string based on the '-' delimiter
    parts = line.split(' - ')
    
    # Extracting date, ID, and type from the parts
    date = parts[0].strip()  # Assuming the date is at the beginning
    id_str = parts[1].strip().split(':')[1].strip()
    type_str = parts[2].strip().split(':')[1].strip()
    fund = parts[3].strip().split(':')[1].strip()

    print(f"[*] Split line: {date} | {id_str} | {type_str} | {fund}")

    return date, id_str, type_str, fund


def read_id_from_file (
        
        date : Optional[str | dt.datetime],
        calculation_type : str,
        fund : Optional[str] = None,
        file_abs_path : Optional[str] = None,
        time_sensitive : bool = True,
        format : str = "%Y-%m-%d",

    ) :
    """
    Returns the ID of calculationtype and fund
    """
    fund = "HV" if fund is None else fund
    file_abs_path = CALCULATION_ID_CSV_ABS_PATH if file_abs_path is None else file_abs_path
    verified_date_dt = date_to_str(date, format)
    
    schema_override = {

        "Date" : pl.Datetime,
        "ID" : pl.Int128,
        "Type" : pl.Utf8,
        "Fundation" : pl.Utf8

    }

    print(verified_date_dt)

    dataframe = pl.read_csv(file_abs_path, schema_overrides=schema_override, columns=list(schema_override.keys()))

    print(dataframe)
    
    # Time-sensitive exact match
    if time_sensitive == True :
        
        date_obj = dt.datetime.strptime(verified_date_dt, format)
        print(date_obj)

        result = dataframe.filter(
            (pl.col("Date") == date_obj) &
            (pl.col("Type") == calculation_type) &
            (pl.col("Fundation") == fund)
        )

    else :

        # Match by date only (ignore time component)
        result = dataframe.filter(
            (pl.col("Date").dt.strftime(format) == verified_date_dt.date()) &
            (pl.col("Type") == calculation_type) &
            (pl.col("Fundation") == fund)
        )

    # Return the ID if found
    if result.height > 0 :
        print(result[0, "ID"])
        return result[0, "ID"]

    # If no match found
    return None


def get_last_run_time (
        
        calculation_type : str,
        fund : str = "HV",
        file_abs_path : str = CALCULATION_ID_FILE_ABS_PATH,
        format : str = "%Y-%m-%d %H:%M:%S"
        
    ) :
    """
    
    """
    last_run_time = None
    last_run_id = None

    # Open the file in read mode
    with open(file_abs_path, 'r') as file :
        
        # Iterate through each line in the file
        for line in file :
            
            # Split the line into date, ID, and calculation type parts
            date_line, id_line, type_line, fund_line = split_line(line)

            # Check if the calculation type matches
            if type_line == calculation_type and fund_line==fund :
            
                # Convert the date string to a datetime object
                current_run_time = date_to_str(date_line, format)

                # Update last_run_time if it is None or the current_run_time is later
                if last_run_time is None or current_run_time > last_run_time :
            
                    last_run_time = current_run_time
                    last_run_id = id_line

    # Return the last run time as a datetime object
    return last_run_time, last_run_id


def get_closest_date_of_run_mv (target_date, filename=CALCULATION_ID_FILE_ABS_PATH, format : str = "%Y-%m-%d %H:%M:%S") :
    """
    
    """
    target_datetime = date_to_str(target_date, format)

    closest_date = None
    closest_id = None
    min_time_diff = None

    with open(filename, 'r') as file :
        
        for line in file:
            
            date_line, id_line, type_line, fund = split_line(line)
            
            if type_line == 'MV':
                
                current_datetime = date_to_str(date_line, format)
                time_diff = abs(current_datetime - target_datetime)
            
                if min_time_diff is None or time_diff < min_time_diff:
            
                    min_time_diff = time_diff
                    closest_date = date_line
                    closest_id = id_line

    return closest_date, closest_id