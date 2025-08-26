import datetime as dt
from typing import Optional, Tuple

from libapi.config.parameters import FILE_ID_CALCULATION_PATH
from libapi.utils.formatter import date_to_str

def write_to_file (
    
        id : str | int,
        date : str | dt.datetime,
        calculation_type : str,
        fund : str = "HV",
        file_abs_path : str = FILE_ID_CALCULATION_PATH,
        format_date : str = "%Y-%m-%d %H:%M:%S"
    
    ) -> None :
    """
    
    """
    if not id :
        return
    
    # Get the current date if not provided
    verified_date = date_to_str(date, format=format_date)
        
    # Check if the date or ID already exists in the file
    if check_duplicate(date, id, fund, file_abs_path) :
        raise ValueError("Error: Date or ID already exists in the file.")

    # Open the file in append mode and write the line
    with open(file_abs_path, 'a') as file :
        file.write(f"{verified_date} - ID: {id} - Type: {calculation_type} - Fund: {fund}\n")


def check_duplicate (

        id : str | int,
        date : str | dt.datetime,
        calculation_type : str,
        fund : str = "HV",
        file_abs_path : str = FILE_ID_CALCULATION_PATH
    
    ) -> bool :
    """
    
    """
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

    return date, id_str, type_str, fund


def read_id_from_file (
        
        date : str | dt.datetime,
        calculation_type : str,
        fund : str = "HV",
        timeSensitive : bool = True,
        format : str = "%Y-%m-%d %H:%M:%S",
        file_abs_path : str = FILE_ID_CALCULATION_PATH

    ) :
    """
    Returns the ID of calculationtype and fund
    """
    verified_date = date_to_str(date, format)
    
    # Open the file in read mode
    with open(file_abs_path, 'r') as file :
    
        # Iterate through each line in the file
        for line in file :
    
            # Split the line into date, ID, and calculation type parts
            date_line, id_line, type_line, fund_line = split_line(line)
            print(line)
    
            # Check if the date and calculation type match
            if date_line == date and type_line == calculation_type and fund_line==fund :
                return id_line  # Return the associated ID
            
            if not timeSensitive and (type_line == calculation_type and date[:-9] == date_line[:-9] and fund_line==fund) :
                return id_line
            
    # If no matching date and calculation type is found, return None
    return None


def get_last_run_time (
        
        calculation_type : str,
        fund : str = "HV",
        file_abs_path : str = FILE_ID_CALCULATION_PATH,
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


def get_closest_date_of_run_mv (target_date, filename=FILE_ID_CALCULATION_PATH, format : str = "%Y-%m-%d %H:%M:%S") :
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