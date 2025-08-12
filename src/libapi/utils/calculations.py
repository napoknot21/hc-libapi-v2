from datetime import datetime, timedelta
from libapi.config.parameters import FILE_ID_CALCULATION_PATH

def write_to_file (date, ID, calculation_type : str, fund : str = "HV", filename : str = FILE_ID_CALCULATION_PATH) -> None :
    """
    
    """
    # Get the current date if not provided
    if not isinstance(date, str) :
        date = date.strftime('%Y-%m-%d %H:%M:%S')
    
    if not ID :
        return
        
    # Check if the date or ID already exists in the file
    if check_duplicate(date, ID, fund, filename) :
        raise ValueError("Error: Date or ID already exists in the file.")

    # Open the file in append mode and write the line
    with open(filename, 'a') as file :
        file.write(f"{date} - ID: {ID} - Type: {calculation_type} - Fund: {fund}\n")


def check_duplicate (date, calculation_type, fund, filename=FILE_ID_CALCULATION_PATH) :
    """
    
    """
    # Open the file in read mode
    with open(filename, 'r') as file :

        # Iterate through each line in the file
        for line in file :
            
            date_line, id_line, type_line, fund_line = split_line(line)
            
            # Check if the date, ID, and calculation type already exist
            if date_line == date and type_line == calculation_type and fund_line==fund :
                # Date, ID, and calculation type already exist
                return True
            
    # If no matching date, ID, or calculation type is found, return False
    return False


def split_line (input_string : str) :
    """
    
    """
    # Split the input string based on the '-' delimiter
    parts = input_string.split(' - ')
    
    # Extracting date, ID, and type from the parts
    date = parts[0].strip()  # Assuming the date is at the beginning
    id_str = parts[1].strip().split(':')[1].strip()
    type_str = parts[2].strip().split(':')[1].strip()
    fund = parts[3].strip().split(':')[1].strip()

    return date, id_str, type_str, fund


def read_id_from_file (date, calculation_type, fund="HV", filename=FILE_ID_CALCULATION_PATH, timeSensitive=True) :
    """
    Returns the ID of calculationtype and fund
    """
    if not isinstance(date, str):
        date = date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Open the file in read mode
    with open(filename, 'r') as file :
    
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


def get_last_run_time (calculation_type, fund="HV", filename=FILE_ID_CALCULATION_PATH) :
    """
    
    """
    last_run_time = None
    last_run_id = None

    # Open the file in read mode
    with open(filename, 'r') as file :
        
        # Iterate through each line in the file
        for line in file :
            
            # Split the line into date, ID, and calculation type parts
            date_line, id_line, type_line, fund_line = split_line(line)

            # Check if the calculation type matches
            if type_line == calculation_type and fund_line==fund :
            
                # Convert the date string to a datetime object
                current_run_time = datetime.strptime(date_line, '%Y-%m-%d %H:%M:%S')

                # Update last_run_time if it is None or the current_run_time is later
                if last_run_time is None or current_run_time > last_run_time :
            
                    last_run_time = current_run_time
                    last_run_id = id_line

    # Return the last run time as a datetime object
    return last_run_time, last_run_id


def get_closest_date_of_run_mv (target_date, filename=FILE_ID_CALCULATION_PATH) :
    """
    
    """
    if not isinstance(target_date, str):
        target_date = target_date.strftime('%Y-%m-%d %H:%M:%S')
    
    target_datetime = datetime.strptime(target_date, '%Y-%m-%d %H:%M:%S')

    closest_date = None
    closest_id = None
    min_time_diff = None

    with open(filename, 'r') as file :
        
        for line in file:
            
            date_line, id_line, type_line, fund = split_line(line)
            
            if type_line == 'MV':
                
                current_datetime = datetime.strptime(date_line, '%Y-%m-%d %H:%M:%S')
                time_diff = abs(current_datetime - target_datetime)
            
                if min_time_diff is None or time_diff < min_time_diff:
            
                    min_time_diff = time_diff
                    closest_date = date_line
                    closest_id = id_line

    return closest_date, closest_id