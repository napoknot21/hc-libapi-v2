from __future__ import annotations

import os
import time
import csv
import polars as pl
import datetime as dt

from typing import Dict, List, Optional, Any

from libapi.utils.formatter import *
from libapi.ice.trade_manager import TradeManager
from libapi.config.parameters import (
    LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_PRICING_BASENAME, LIBAPI_LOGS_PRICER_COLUMNS,
    FREQUENCY_DATE_MAP, EQ_PRICER_CALC_PATH, RISKS_UNDERLYING_ASSETS,
    COLUMNS_IN_PRICER, 

)

class Pricer :


    def __init__ (self, trade_manager : Optional[TradeManager] = None) -> None :
        """
        
        """
        self.api = trade_manager if trade_manager is not None else TradeManager()

    # -------- API payload helpers --------

    def generate_payload_api (
            
            self,
            id : int,
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
        verified_settl_date = date_to_str(settl_date)
        verified_expiry_date = date_to_str(expiry_date)

        payload = {

            "ID" : id,

            "InstrumentType" : instr_type,

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
            asset_class : str,
            asset_dict : Optional[Dict] = None,
            time : Optional[str | dt.time] = None,
            date : Optional[str | dt.datetime | dt.date] = None,
            valuation_type : str = "EOD",
            default_risks : List[str] = ["Spread", "Theta"],
            instr_type : str = "Vanilla",
            underly_asset : Optional[str | Dict] = None,
            payout_ccy : str = "EUR",
            endpoint : Optional[str] = None,

        ) -> Optional[Dict] :
        """
        Calculatees the price via the ICE API for an EQ
        
        Args:

        """
        endpoint = EQ_PRICER_CALC_PATH if endpoint is None else endpoint
        asset_dict = RISKS_UNDERLYING_ASSETS if asset_dict is None else asset_dict
        
        verfied_date = date_to_str(date) if date is not None else None
        verfied_time = time_to_str(time) if time is not None else None

        instruments_payload = []
        
        index_len = 0
        for instrument in (instruments) :
            
            if instrument.get("ID") is None :
                instrument['ID'] = index_len

            instrument_payload = self.generate_payload_api(

                instrument["ID"],
                instrument["direction"],
                instrument['opt_type'],
                instrument["strike"],
                instrument['notional'],
                instrument['expiry'],
                instrument['SettlementDate'],
                instr_type=instr_type

            )

            if asset_class == "FX" :

                base_ccy = underly_asset[:-3]
                term_ccy = underly_asset[-3:]

                underlying_asset = {

                    "BaseCurrency" : base_ccy,
                    "TermCurrency" : term_ccy

                }

            elif asset_class == "EQ" :
                
                underlying_asset = {

                    "BBGTicker" : instrument["BBGTicker"]

                }
                
            else : # Here we are in "Basket case" (Yes, Green Day's reference)

                instrument_payload["PayoutCurrency"] = payout_ccy
                underlying_asset = instrument["underlyingAssets"]

            
            instrument_payload["UnderlyingAssets"] = underlying_asset

            instruments_payload.append(instrument_payload)
            index_len += 1

        self.log_api_call((index_len + 1)) # Log the lenght of the instruments table

        valuation = {

            "type" : valuation_type,
            "Date" : verfied_date

        }

        artifacts = {

            'instruments' : default_risks,
            'underlyingAssets' : asset_dict.get(asset_class)

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
            n_instruments : str | int,
            date : Optional[str | dt.datetime] = None,
            format : str = "%Y-%m-%d %H:%M:%S",
            log_abs_path : Optional[str] = None
        
        ) -> None :
        """
        Log an API call with the current timestamp and number of instruments.
        """
        LIBAPI_LOGS_PRICING_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_PRICING_BASENAME)
        log_abs_path = LIBAPI_LOGS_PRICING_ABS_PATH if log_abs_path is None else log_abs_path
        
        date =  date_to_str(date)

        obj_date = dt.datetime.strptime(date, format[:8])
        obj_time = dt.datetime.now().time()

        obj_datetime = dt.datetime.combine(obj_date, obj_time)

        start = time.time()
        
        # Create new row as Polars DataFrame
        new_row = {
            
            "Date": dt.datetime.strftime(obj_datetime, format),
            "n_instruments": int(n_instruments)
        
        }

        # Check if file exists
        file_exists = os.path.isfile(log_abs_path)

        with open(log_abs_path, mode="a", newline="", encoding="utf-8") as f :
        
            writer = csv.DictWriter(f, fieldnames=new_row.keys())
            
            if not file_exists :
                writer.writeheader()

            writer.writerow(new_row)

        print(f"[+] Information written in the CSV log file into {time.time() - start} seconds")


    def flatten_pricer_response (
            
            self,
            response : Dict,
            instruments : List[Dict]

        ) -> Optional[pl.DataFrame] :
        """
        Vectorized + Polars-native flattener for pricer JSON (Dict) response.


        """
        instrument_list = response.get('instruments', [])

        if not instrument_list :

            print("[-] Empty instrument list...")
            return None
        
        base = pl.DataFrame(instrument_list)

        # Keep minimal base identifiers if present
        keep_cols = [c for c in ("id",) if c in base.columns]

        if not keep_cols :
            
            print("[!] No ID. Nothing to join on. Returning a safe frame")
            return pl.DataFrame()

        # ---------- Flatten top-level results ----------

        base = base.select(keep_cols + [c for c in base.columns if c in ("results", "assets")])
        
        # We'll build a list of flattened rows as dicts
        flattened_rows = []

        for row in base.iter_rows(named=True) :

            # Start from basic keys (like id)
            flat_row = {k: row[k] for k in keep_cols if k in row}

            # Flatten 'results' list
            results = row.get('results')
            
            if isinstance(results, list) :

                for result_dict in results :

                    code = result_dict.get('code')
                    value = result_dict.get('value')
                    
                    if code is not None and value is not None :
                        flat_row[code] = value

                    if 'currency' in result_dict and code is not None :
                        flat_row[f"{code}_currency"] = result_dict['currency']

            # Flatten 'assets' list (only first asset)
            assets = row.get('assets')
            
            if isinstance(assets, list) and len(assets) > 0 and isinstance(assets[0], dict):
            
                first_asset = assets[0]
                flat_row['asset'] = first_asset.get('name')
                asset_results = first_asset.get('results', [])
            
                for result_dict in asset_results :
            
                    code = result_dict.get('code')
                    value = result_dict.get('value')

                    if code is not None and value is not None :
                        flat_row[code] = value
                    
                    if 'currency' in result_dict and code is not None :
                        flat_row[f"{code}_currency"] = result_dict['currency']

            flattened_rows.append(flat_row)

        # Create Polars DataFrame from flattened dicts
        data = pl.DataFrame(flattened_rows)

        # Prepare instruments DataFrame
        instruments_df = pl.DataFrame(instruments)

        # Columns to join on (only keep existing)
        join_cols = [col for col in ['ID', 'direction', 'pair', 'opt_type', 'strike',
                                    'notional', 'notional_currency', 'expiry',
                                    'BBGTicker', 'stratid'] if col in instruments_df.columns]

        # Join data on id = ID
        # Polars requires join keys with same names or specify them explicitly:
        if 'id' in data.columns and 'ID' in instruments_df.columns :
        
            instruments_df_renamed = instruments_df.rename({'ID': 'id'})
            merged = data.join(instruments_df_renamed.select(['id'] + join_cols[1:]), on='id', how='left')
        
        else :
            
            print("[!] Cannot join on id/ID: columns missing.")
            return data

        return merged
        """
        
        
        """
        df  = pl.DataFrame(response.get("instruments", []))
        lf  = df.select([c for c in ("id", "results", "assets") if c in df.columns]).lazy()

        top = (
        
            lf
            .select("id", pl.col("results").alias("r"))
            .explode("r").drop_nulls("r")
            .select(

                "id",
                pl.col("r").struct.field("code").alias("code"),
                pl.col("r").struct.field("value").alias("value"),
                pl.col("r").struct.field("currency").alias("currency"),
                pl.lit(0).alias('src')
            
            )
        
        )

        fa = (lf.select("id",
            pl.when(pl.col("assets").is_not_null() & (pl.col("assets").list.lengths()>0))
              .then(pl.col("assets").list.get(0)).otherwise(None).alias("a")))
        
        asset = fa.select("id", pl.col("a").struct.field("name").alias("asset")).unique()
        aset = (fa.select("id", pl.col("a").struct.field("results").alias("r"))
              .explode("r").drop_nulls("r")
              .select("id",
                      pl.col("r").struct.field("code").alias("code"),
                      pl.col("r").struct.field("value").alias("value"),
                      pl.col("r").struct.field("currency").alias("currency"),
                      pl.lit(1).alias("src")))

        # pick best per (id,code): asset wins, then last seen
        comb = pl.concat([top, aset], how="vertical_relaxed").with_row_count("rn")
        prio = (pl.col("src").cast(pl.Int64())*pl.lit(10**12) + pl.col("rn").cast(pl.Int64()))
        best = (comb.group_by(["id","code"])
                    .agg(value=pl.col("value").take(prio.arg_max()),
                        currency=pl.col("currency").take(prio.arg_max())))

        # wide values + currencies
        valw = best.pivot(values="value", index="id", columns="code")
        curw = (best.pivot(values="currency", index="id", columns="code")
                 .select([pl.col("id"), pl.all().exclude("id").name.suffix("_currency")]))

        out = (lf.select("id").unique().join(asset, on="id", how="left")
             .join(valw, on="id", how="left")
             .join(curw, on="id", how="left"))


        if instruments :

            instr = pl.DataFrame(instruments)
            
            list_columns = list(columns_overrides.keys())
            keep = [c for c in list_columns if c in instr.columns]

            if keep:
                
                out = out.join(

                    instr
                    .select(keep)

                    .unique(
                        subset=["ID"],
                        keep="last"
                    ).lazy(),

                    left_on="id",
                    right_on="ID",
                    how="left"

                )
        
        return out.collect()


    def split_list (self, list : List[Any], max_num : int) -> List[List[Any]]:
        """
        Splits a list into smaller sublists (chunks) of a specified maximum size.

        Args:
            items (List[Any]): The list of elements to split.
            max_num (int): The maximum number of elements per chunk.

        Returns:
            List[List[Any]]: A list of sublists, each containing at most `max_num` elements.
                            If the original list length is not evenly divisible by `max_num`,
                            the last chunk will contain the remaining elements.
        """
        # Calculate the number of parts needed based on the maximum number of elements per part
        chunks = [list[i:i + max_num] for i in range(0, len(list), max_num)]

        return chunks


    def generate_dates (
            
            self,
            start_date : Optional[str | dt.datetime] = None,
            end_date : Optional[str | dt.datetime] = None,
            frequency : str = "Day",
            frequency_map : Optional[Dict] = None,
            format : str = "%Y-%m-%d"
        
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
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

        start_date = dt.datetime.strptime(start_date, format)
        end_date = dt.datetime.strptime(end_date, format)

        frequency_map = FREQUENCY_DATE_MAP if frequency_map is None else frequency_map
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
        series_dates_wd = series_dates.filter(series_dates.dt.weekday() <= 5)
        
        if series_dates_wd.len() == 0 :

            print("[*] No week day in the generated list after filter. Returning an empty List")
            return []

        # Convert the date range to a list of strings in the format 'YYYY-MM-DD'
        range_date_list = (series_dates_wd
            .to_frame("dates")
            .with_columns(pl.col("dates").dt.strftime(format).alias("formatted_dates"))["formatted_dates"]
            .to_list()
        )

        print("[+] Successfully range date generated and converted to list")

        return range_date_list
    
