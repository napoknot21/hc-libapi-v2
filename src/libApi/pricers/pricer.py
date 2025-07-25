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


    def treat_json_response_pricer (self, json_response, instruments) :
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
