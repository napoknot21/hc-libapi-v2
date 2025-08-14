from __future__ import annotations

import os
import time
import polars as pl
import datetime as dt
import pandas as pd

from typing import Dict, List, Optional

from libapi.ice.trade_manager import TradeManager
from libapi.config.parameters import PRICING_LOG_FILE_PATH, FREQUENCY_DATE_MAP, EQ_PRICER_CALC_PATH, RISKS_UNDERLYING_ASSETS


def _as_date_str (date : str | dt.datetime = None) -> str :
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


def _as_time_str (time : str | dt.time = None) -> str :
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


class Pricer :


    def __init__ (self, trade_manager : TradeManager = None) -> None :
        """
        
        """
        self.api = trade_manager if trade_manager is not None else TradeManager()


    def generate_payload_api (
            
            self,
            id : int,
            bbg_ticker : int,
            direction : str,
            opt_type : str,
            strike : int,
            notional : int,
            expiry_date : str | dt.datetime,
            settl_date :  str | dt.datetime,
            instr_type : str = "Vanilla",
            style : str = "European"

        ) -> Optional[Dict] :
        """
        Function that creates the correct format attended by the API.

        Asumes that all parameters are correct or not None (checks the date format)
        """
        verified_settl_date = _as_date_str(settl_date)
        verified_expiry_date = _as_date_str(expiry_date)

        underly_asset = { "BBGTicker" : bbg_ticker }

        payload = {

            "ID" : id,

            "InstrumentType" : instr_type,
            "UnderlyingAsset" : underly_asset,

            "BuySell" : direction,
            "CallPut" : opt_type,

            "Strike" : strike,
            "Notional" : notional,

            "ExpiryDate" : verified_expiry_date,
            "SettlementDate": verified_settl_date,

            "Style": style

        }
    
        return payload


    def request_prices_api (
            
            self,
            instruments : List[Dict],
            time : str | dt.time,
            date : str | dt.datetime,
            asset_class : str,
            asset_dic : Dict = RISKS_UNDERLYING_ASSETS,
            valuation_type : str = "EOD",
            endpoint : str = EQ_PRICER_CALC_PATH,
            default_risks : List = ['Spread', 'Theta']

        ) -> Optional[Dict] :
        """
        Calculatees the price via the ICE API for an EQ
        
        Args:

        """
        verfied_date = _as_date_str(date) if date is not None else None
        verfied_time = _as_time_str(time) if time is not None else None

        instruments_payload = []

        index_len = 0
        for instrument in (instruments) :
            
            if instrument.get("ID") is None :
                instrument['ID'] = index_len

            instrument_payload = self.generate_payload_api(

                instrument["ID"],
                instrument["BGGTicker"],
                instrument["direction"],
                instrument['opt_type'],
                instrument["strike"],
                instrument['notional'],
                instrument['expiry'],
                instrument['SettlementDate']

            )

            index_len += 1

        self.log_api_call((index_len + 1)) # Log the lenght of the instruments table

        valuation = {

            "type" : valuation_type,
            "Date" : verfied_date

        }


        artifacts = {

            'instruments' : default_risks,
            'underlyingAssets' : asset_dic.get(asset_class)

        }

        response = self.api.post(

            endpoint=endpoint,
            data={

                "valuation" : valuation,
                "artifacts" : artifacts,
                "instruments" : instruments_payload

            }

        )

        return response

    
    def log_api_call (
        
            self,
            n_instruments : int,
            date : str | dt.datetime = dt.datetime.now(),
            pricing_abs_path : str = PRICING_LOG_FILE_PATH
        
        ) -> None :
        """
        Log an API call with the current timestamp and number of instruments.
        """
        formatted_date =  _as_date_str(date) if date is not None else _as_date_str()

        start = time.time()
        
        # Create new row as Polars DataFrame
        new_row = pl.DataFrame(
            {
                "date": [formatted_date],
                "n_instruments": [n_instruments]
            }
        )

        if os.path.exists(pricing_abs_path) :
            
            # File exists
            logs = pl.read_csv(pricing_abs_path)
            logs = pl.concat([logs, new_row], how="vertical")

        else :

            # File does not exists
            logs = new_row

        print("[+] Log file successfully updated for API call")

        logs.write_csv(pricing_abs_path)

        print(f"[-] Information written in the CSV log file into {time.time() - start} seconds")


    def flatten_pricer_json (
            
            self,
            json_response : Dict,
            instruments : List[Dict]

        ) -> Optional[pl.DataFrame] :
        """
        
        """
        instrument_list = json_response.get('instruments', [])

        if not instrument_list :
            print("[-] Empty instrument list...")
            return None
        
        base = pl.DataFrame(instrument_list)

        # Keep minimal base identifiers if present
        keep_cols = [c for c in ["id"] if c in base.columns]
        base = base.select(keep_cols + [c for c in base.columns if c in ("results", "assets")])

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
    

    def generate_dates (
            
            self,
            start_date : str | dt.datetime = dt.datetime.now(),
            end_date : str | dt.datetime = dt.datetime.now(),
            frequency : str = "Day",
            frequency_map : Dict = FREQUENCY_DATE_MAP
        
        ) -> Optional[List]:
        """
        Function that returns a list of dates based on the start date, end date and frequency

        Args:
            start_date (str): start date in format 'YYYY-MM-DD'
            end_date (str): end date in format 'YYYY-MM-DD'
            frequency (str): 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
            
        Returns:
            list: list of dates in format 'YYYY-MM-DD' or None
        """
        interval = frequency_map.get(frequency)

        if interval is None :

            print(f"[-] Invalid frequency: {frequency}. Choose from 'Day', 'Week', 'Month', 'Quarter', 'Year'.")
            return None

        # This return a Series
        try :

            series_dates = pl.date_range(start_date, end_date, interval=interval, eager=True)

        except Exception as e :
            
            print(f"[-] Error generating dates: {e}")
            return None

        if series_dates.len() == 0 :

            print("[-] Error during generation: empty range (check start & end).")
            return None
        
        # Filter out weekends for non-business day frequencies
        series_dates_wd = series_dates.filter(series_dates.dt.weekday() <= 4)
        
        if series_dates_wd.len() == 0 :

            print("[*] No week day in the generated list after filter. Returning an empty List")
            return []

        # Convert the date range to a list of strings in the format 'YYYY-MM-DD'
        range_date_list = series_dates_wd.strftime('%Y-%m-%d').tolist()

        print("[+] Successfully range date generated and converted to list")

        return range_date_list
    

    def _valide_date (self, date_str : str) -> dt.datetime :
        """
        Valide date format and convert to datime

        Args:
            date_str (str) : The string date format to check

        Return:
            
        """
        try :

            return dt.datetime.strptime(date_str, "%Y-%m-%d")
        
        except ValueError :

            return None

