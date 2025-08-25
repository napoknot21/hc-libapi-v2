from __future__ import annotations

import os
import time
import polars as pl
import datetime as dt
#import pandas as pd

from typing import Dict, List, Optional, Any

from libapi.utils.formatter import *
from libapi.ice.trade_manager import TradeManager
from libapi.config.parameters import (
    PRICING_LOG_FILE_PATH, FREQUENCY_DATE_MAP, EQ_PRICER_CALC_PATH, RISKS_UNDERLYING_ASSETS,
    COLUMNS_IN_PRICER, API_PRICER_LOG_SCHEMA

)

class Pricer :


    def __init__ (self, trade_manager : TradeManager | None = None) -> None :
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
            asset_dict : Dict = RISKS_UNDERLYING_ASSETS,
            time : str | dt.time | None = None,
            date : str | dt.datetime | None = None,
            valuation_type : str = "EOD",
            default_risks : List = ['Spread', 'Theta'],
            endpoint : str = EQ_PRICER_CALC_PATH,
            instr_type : str = "Vanilla",

            underly_asset : str | Dict = None,

            payout_ccy : str = "EUR"

        ) -> Optional[Dict] :
        """
        Calculatees the price via the ICE API for an EQ
        
        Args:

        """
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
                
            else : # Here we are in "Basket case" (Green Day's reference)

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
            n_instruments : int,
            date : str | dt.datetime = None,
            schema_override : Dict = API_PRICER_LOG_SCHEMA,
            log_abs_path : str = PRICING_LOG_FILE_PATH
        
        ) -> None :
        """
        Log an API call with the current timestamp and number of instruments.
        """
        formatted_date =  date_to_str(date)

        start = time.time()
        
        # Create new row as Polars DataFrame
        new_row = pl.DataFrame(
            {
                "date": [formatted_date],
                "n_instruments": [n_instruments]
            }
        )

        if os.path.exists(log_abs_path) :
            
            # File exists
            

            logs = pl.read_csv(log_abs_path, schema_overrides=schema_override)
            logs = pl.concat([logs, new_row], how="vertical")

        else :

            # File does not exists
            logs = new_row

        print("[+] Log file successfully updated for API call")

        logs.write_csv(log_abs_path)

        print(f"[-] Information written in the CSV log file into {time.time() - start} seconds")


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
    

    def treat_json_response_pricer_polars(
        
            self,
            response: Dict[str, Any],
            instruments: List[Dict[str, Any]]
        
        ) -> pl.DataFrame :
        """
        
        """
        # Base frame from JSON
        base = pl.DataFrame(response["instruments"])

        # Keep only what we need early (reduces memory).
        keep = [c for c in ("id", "results", "assets") if c in base.columns]
        base = base.select(keep)
        lbase = base.lazy()


        # --- Top-level results -> long
        # results: list[struct{code, value, currency}]
        if "results" in base.columns :

            results_long = (

                lbase
                .select(["id", "results"])
                .explode("results")
                .drop_nulls("results")

                .select(

                    pl.col("id"),
                    pl.col("results").struct.field("code").alias("code"),
                    pl.col("results").struct.field("value").alias("value"),
                    pl.col("results").struct.field("currency").alias("currency"),
                    pl.lit(0).alias("src_order")  # 0 = top-level

                )
            )
        
        else :

            results_long = pl.DataFrame(
                
                {
                    "id" : [],
                    "code" : [],
                    "value" : [],
                    "currency" : [],
                    "src_order" : []
                }

            ).lazy()

        # --- First asset name & results -> long
        if "assets" in base.columns :

            assets_pre = (

                lbase
                .select(["id", "assets"])
                .with_columns(

                    first_asset = pl.when(

                        pl.col(
                            "assets".is_not_null() & (pl.col('assets').list.lengths() > 0)
                        )

                    ).then(pl.col("assets").list.get(9)).otherwise(None)

                )

            )

            asset_name = (
                
                assets_pre
                .select("id", pl.col("first_asset").struct.field("name").alias("asset"))
                .unique()

            )

            asset_results_long = (

                assets_pre
                .select("id", pl.col("first_asset").struct.field("results").alias("asset_res"))
                .explode("asset_res")
                .drop_nulls("asset_res")

                .select(

                    pl.col("id"),
                    pl.col("asset_res").struct.field("code").alias("code"),
                    pl.col("asset_res").struct.field("value").alias("value"),
                    pl.col("asset_res").struct.field("currency").alias("currency"),
                    pl.lit(1).alias("src_order")  # 1 = asset-level (preferred)

                )

            )

        else :

            asset_name = pl.DataFrame(

                {
                    "id": [],
                    "asset": []
                }

            ).lazy()

            asset_results_long = pl.DataFrame(
            
                {
                    "id": [],
                    "code": [],
                    "value": [],
                    "currency": [],
                    "src_order": []
                }
            
            ).lazy()

        # Combine
        combined = pl.concat([results_long, asset_results_long], how="vertical_relaxed")

        





        # First asset -> asset results long + asset name
        assets_pre = (
            base
            .select(["id", "assets"])
            .with_columns(
                first_asset = pl.when(
                    pl.col("assets").is_not_null() & (pl.col("assets").list.lengths() > 0)
                ).then(pl.col("assets").list.get(0)).otherwise(None)
            )
            .with_columns(
                asset_name = pl.col("first_asset").struct.field("name"),
                asset_res  = pl.col("first_asset").struct.field("results")
            )
        )

        asset_name = assets_pre.select(["id", "asset_name"]).unique()

        asset_results_long = (
            assets_pre
            .select(["id", "asset_res"])
            .with_columns(pl.col("asset_res").list.explode().alias("result"))
            .drop_nulls("result")
            .with_columns(
                code     = pl.col("result").struct.field("code"),
                value    = pl.col("result").struct.field("value"),
                currency = pl.col("result").struct.field("currency"),
                src_order= pl.lit(1)  # 1 = asset-level (preferred)
            )
            .select(["id", "code", "value", "currency", "src_order"])
        )

        # Combine and prefer asset-level values on collisions via aggregation='last'
        combined_long = pl.concat([results_long, asset_results_long], how="vertical_relaxed") \
                        .sort(["id", "code", "src_order"])

        values_wide = combined_long.pivot(
            index="id", columns="code", values="value", aggregate_function="last"
        )

        currency_wide = combined_long.pivot(
            index="id", columns="code", values="currency", aggregate_function="last"
        )
        # Suffix currency columns
        currency_wide = currency_wide.rename({c: f"{c}_currency" for c in currency_wide.columns if c != "id"})

        # Stitch everything together
        out = (
            base.select("id").unique()
            .join(asset_name.rename({"asset_name": "asset"}), on="id", how="left")
            .join(values_wide, on="id", how="left")
            .join(currency_wide, on="id", how="left")
        )

        # Join with instruments
        if instruments:
            instr = pl.DataFrame(instruments)
            sel = [c for c in ['ID','direction','pair','opt_type','strike','notional',
                            'notional_currency','expiry','BBGTicker','stratid']
                if c in instr.columns]
            if sel:
                out = out.join(instr.select(sel), left_on="id", right_on="ID", how="left")

        return out


    def flatten_json_response (self, response : Dict, instruments : List[Dict]) :
        """
        
        """
        instruments_df = pl.DataFrame(instruments)
        instruments_data = pl.DataFrame(response.get("instruments", []))

        if instruments_df is None or instruments_data is None :

            return pl.DataFrame([])
        
        # Early detection of missing "results"
        if all("results" not in instrument for instrument in instruments_data) :

            return pl.DataFrame(instruments_data)
        

        # Process each instrument
        flattened_rows = []
        for instrument in instruments_data :

            flat_row = {k :v for k, v in instrument.items() if k not in ["results", "assets"]}

            # Handle top_level results
            for result in instrument.get("results", []) :

                return None

        return None

    # OPTIMISED FUNCTOIN ?
    def flatten_response_json (self, response, instruments, columns_overrides : Dict = COLUMNS_IN_PRICER) :
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
    
