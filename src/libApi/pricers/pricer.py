import pandas as pd
from datetime import datetime

from libApi.ice.trade_manager import TradeManager

PRINCING_LOG_FILE="" # from PARAMETERS


class Pricer :

    def __init__ (self) :
        self.api = TradeManager()

    
    def log_api_call (self, n_instruments) -> None :
        """
        
        """
        logs = pd.read_csv(PRINCING_LOG_FILE)
        logs = logs._append(
            {
                "date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "n_instruments" : n_instruments
            },
            ignore_index=True
        )

        logs.to_csv(PRINCING_LOG_FILE, index=False)


    def treat_json_response_pricer (self, json_response, instruments) :
        """
        
        """
        data = pd.json_normalize(json_response['instruments']) 

        if "results" not in data.columns:
            return data

        def treat_row(row) :
            if 'results' in row and isinstance(row['results'], list):
                for result_dict in row['results']:
                    code = result_dict.get('code')
                    value = result_dict.get('value')
                    if code is not None and value is not None:
                        row[code] = value
                    if "currency" in result_dict:
                        row[code + "_currency"] = result_dict['currency']
            if "assets" in row and isinstance(row['results'], list):
                if not type(row['assets'])==float and len(row['assets'][0])>0:
                    row['asset'] = row['assets'][0]['name']
                    for result_dict in row['assets'][0]['results']:
                        code = result_dict.get('code')
                        value = result_dict.get('value')
                        if code is not None and value is not None:
                            row[code] = value
                        if "currency" in result_dict:
                            row[code + "_currency"] = result_dict['currency']
            return row

        data = data.apply(lambda row: treat_row(row), axis=1)
        data.drop(columns=[col for col in ['results', 'assets'] if col in data.columns], inplace=True)

        instruments_df = pd.DataFrame(instruments)
        data = pd.merge(
            data, 
            instruments_df[[col for col in ['ID', 'direction', 'pair', 'opt_type', 'strike', 'notional', 'notional_currency', 'expiry', 'BBGTicker', 'stratid'] if col in instruments_df.columns]], 
            left_on='id', right_on='ID', how='left')
        
        return data