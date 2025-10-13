from __future__ import annotations

import os
import polars as pl
import datetime as dt

from functools import partial
from typing import Dict, List, Optional, Tuple

from libapi.config.parameters import COLUMNS_IN_PRICER, SAVED_REQUESTS_DIRECTORY_PATH, EQ_PRICER_CALC_PATH, RISKS_UNDERLYING_ASSETS
from libapi.pricers.pricer import Pricer
from libapi.utils.formatter import date_to_str


class PricerBasket (Pricer) :


    def __init__ (self,) -> None :
        super().__init__()

    
    def request_basket_price_api (
        
            self,
            basket : Dict,
            asset_dict : Optional[Dict] = None,
            date : Optional[str | dt.date | dt.datetime] = None,
            endpoint : Optional[str] = None,
            instr_type : str = "Basket",
            payout_ccy : str = "EUR"

        ) -> pl.DataFrame :
        """
        
        """
        asset_dict = RISKS_UNDERLYING_ASSETS if asset_dict is None else asset_dict
        endpoint = EQ_PRICER_CALC_PATH if endpoint is None else endpoint
        date = date_to_str(date)

        response = super().request_prices_api(

            instruments=[basket],
            asset_class="Basket",
            asset_dict=asset_dict,
            date=date,
            endpoint=endpoint,
            instr_type=instr_type,
            payout_ccy=payout_ccy

        )
        
        response_df = self.treat_json_response_pricer(response, [basket])

        return response_df


    def equity_curve (
            
            self,
            basket : Dict,
            start_date : Optional[str | dt.date | dt.datetime] = None,
            end_date : Optional[str | dt.date | dt.datetime] = None,
            frequency : str = "Day",
            request_abs_dir : Optional[str] = None
        
        ) :
        """
        
        Args:
            basket (dict) : 
            start_date (str) : Starting date, in format 'YYYY-MM-DD'
            end_date (str) : End date, in format "YYYY-MM-DD"
            frequency (str) : Frequency of the equity curve as "Day", "Week", "Month", "Quarter", "Year".

        """
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

        request_abs_dir = SAVED_REQUESTS_DIRECTORY_PATH if request_abs_dir is None else request_abs_dir
        
        # Get the dates based on start_date and end_date for a given frequency
        valuation_dates = self.generate_dates(start_date, end_date, frequency)

        # Check if this request has been cached
        exists, filename = self.does_equity_curve_exist(basket, start_date, end_date, frequency)

        full_path = os.path.join(request_abs_dir, filename)
        
        if exists :
            return pl.read_excel(full_path)

        # Initialize an empty DataFrame to store the results
        all_prices = pl.DataFrame()

        # Request pricing for each date
        for date in valuation_dates :

            prices = self.request_basket_price_api(basket, date=date)
            prices['ValuationDate'] = date
            
            all_prices = pl.concat([all_prices, prices])

        # Save as file in the database
        all_prices.to_excel(full_path, index=False)

        # Return the equity curve
        return all_prices
    

    def does_equity_curve_exist (
            
            self,
            basket : Dict,
            start_date : Optional[str | dt.date | dt.datetime] = None,
            end_date : Optional[str | dt.date | dt.datetime] = None,
            frequency : str = "Day",
            request_abs_dir : Optional[str] = None
        
        ) -> Tuple[bool, str] :
        """
        
        Args:
            basket () : 
            start_date (str) :  Starting date, in format "YYYY-MM-DD"
            end_date (str) : Ending date, in format "YYYY-MM-DD"
            frequency (str) : Frequency for date like "Day", "Month", "Year", "Quarter"

        Returns:

        """
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

        request_abs_dir = SAVED_REQUESTS_DIRECTORY_PATH if request_abs_dir is None else request_abs_dir

        filename = f"equity_curve_{basket['buySell']}_{basket['payoutCurrency']}_strike-{basket['strike']}_expi-{basket['expiryDate']}_from-{start_date}_to-{end_date}_each-{frequency}.xlsx"
        
        return filename in os.listdir(request_abs_dir), filename
    


