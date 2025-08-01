import pandas as pd
from datetime import datetime

from libApi.ice.trade_manager import TradeManager
from libApi.config.parameters import PRICING_LOG_FILE_PATH

class Pricer :

    def __init__ (self) :
        self.api = TradeManager()

    
    def log_api_call (self, n_instruments) -> None :
        """
        
        """
        try:
            logs = pd.read_csv(PRICING_LOG_FILE_PATH)

        except FileNotFoundError:

            logs = pd.DataFrame(columns=["date", "n_instruments"])

        new_row = pd.DataFrame(
            [
                {
                    "date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "n_instruments" : n_instruments
                }
            ]
        )
        
        logs = pd.concat([logs, new_row], ignore_index=True)
        logs.to_csv(PRICING_LOG_FILE_PATH, index=False)


    def treat_json_response_pricer (self, json_response, instruments) -> pd.DataFrame :
        """
        
        """
        data = pd.json_normalize(json_response['instruments']) 

        if "results" not in data.columns :
            return data

        def treat_row(row) :
            """
            
            """
            if 'results' in row and isinstance(row['results'], list) :

                for result_dict in row['results'] :

                    code = result_dict.get('code')
                    value = result_dict.get('value')

                    if code is not None and value is not None :
                        row[code] = value

                    if "currency" in result_dict :
                        row[code + "_currency"] = result_dict['currency']

            if "assets" in row and isinstance(row['results'], list) :

                if not type(row['assets'])==float and len(row['assets'][0]) > 0 :

                    row['asset'] = row['assets'][0]['name']

                    for result_dict in row['assets'][0]['results'] :

                        code = result_dict.get('code')
                        value = result_dict.get('value')

                        if code is not None and value is not None :
                            row[code] = value

                        if "currency" in result_dict :
                            row[code + "_currency"] = result_dict['currency']

            return row

        data = data.apply(lambda row: treat_row(row), axis=1)
        data.drop(columns=[col for col in ['results', 'assets'] if col in data.columns], inplace=True)

        instruments_df = pd.DataFrame(instruments)
        
        data = pd.merge(
            data, 
            instruments_df[[col for col in ['ID', 'direction', 'pair', 'opt_type', 'strike', 'notional', 'notional_currency', 'expiry', 'BBGTicker', 'stratid'] if col in instruments_df.columns]], 
            left_on='id',
            right_on='ID',
            how='left'
        )
        
        return data
    

    def split_list (self, lst, max_num) :
        """
        
        """
        # Calculate the number of parts needed based on the maximum number of elements per part
        num_parts = len(lst) // max_num + (1 if len(lst) % max_num != 0 else 0)
        
        # Initialize the starting index
        start = 0
        
        # Iterate through each part
        parts = []
        for _ in range(num_parts) :

            # Calculate the end index for the current part
            end = min(start + max_num, len(lst))
            parts.append(lst[start:end])
            
            start = end
        
        return parts
    

    def get_dates (self, start_date : str, end_date : str, frequency="Day") :
        """
        Function that returns a list of dates based on the start date, end date and frequency

        Args:
            start_date (str): start date in format 'YYYY-MM-DD'
            end_date (str): end date in format 'YYYY-MM-DD'
            frequency (str): 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
            
        Returns:
            list: list of dates in format 'YYYY-MM-DD'
        """
        start = self._valide_date(start_date)
        end = self._valide_date(end_date)

        if start is None or end is None :
            raise ValueError(f"[-] Invalid date format. Must be 'YYYY-MM-DD'.")
        
        if start > end :
            raise ValueError("[-] Start_date must be before or equal to end_date.")

        # Map the frequency string to pandas offset alias
        freq_map = {
            'Day' : 'D',
            'Week' : 'W',
            'Month' : 'ME', # Month end
            'Quarter' : 'Q',
            'Year' : 'Y'
        }

        pd_frequency = freq_map.get(frequency)

        if pd_frequency is None :
            raise ValueError(f"[-] Invalid frequency: {frequency}. Choose from 'Day', 'Week', 'Month', 'Quarter', 'Year'.")
        
        # Generate the date range using pandas
        date_range = pd.date_range(start=start, end=end, freq=pd_frequency)

        # Filter out weekends for non-business day frequencies
        if frequency == 'Day' :
            date_range = date_range[~date_range.weekday.isin([5, 6])]

        # Convert the date range to a list of strings in the format 'YYYY-MM-DD'
        date_list = date_range.strftime('%Y-%m-%d').tolist()

        # Add start date if it is not in the dates
        if start_date < date_list[0] :
            date_list.insert(0, start_date)

        return date_list
    

    def _valide_date (self, date_str : str) -> datetime :
        """
        Valide date format and convert to datime

        Args:
            date_str (str) : The string date format to check

        Return:
            
        """
        try :

            return datetime.strptime(date_str, "%Y-%m-%d")
        
        except ValueError :

            return None

