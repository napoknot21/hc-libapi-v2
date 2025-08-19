import os
import polars as pl
import datetime as dt

from functools import partial
from typing import Dict, List, Optional

from libapi.config.parameters import COLUMNS_IN_PRICER, SAVED_REQUESTS_DIRECTORY_PATH, EQ_PRICER_CALC_PATH, RISKS_UNDERLYING_ASSETS
from libapi.pricers.pricer import Pricer


def _as_date_str (date : str | dt.datetime) -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    return date.strftime("%Y-%m-%d") if isinstance(date, dt.datetime) else str(date)


class PricerBasket (Pricer) :


    def __init__ (self,) -> None :
        super().__init__()

    
    def request_basket_price_api (
        
            self,
            basket : Dict,
            asset_dict : Dict = RISKS_UNDERLYING_ASSETS,
            date : str | dt.datetime = None,
            endpoint : str = EQ_PRICER_CALC_PATH,
            instr_type : str = "Basket",
            payout_ccy : str = "EUR"

        ) -> pl.DataFrame :
        """
        
        """
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


    def equity_curve (self, basket: dict, start_date : str, end_date : str, frequency="Day") :
        """
        
        Args:
            basket (dict) : 
            start_date (str) : Starting date, in format 'YYYY-MM-DD'
            end_date (str) : End date, in format "YYYY-MM-DD"
            frequency (str) : Frequency of the equity curve as "Day", "Week", "Month", "Quarter", "Year".

        """
        if not isinstance(start_date) or not isinstance(end_date) : 
            raise KeyError("[-] Dates must be in string format : YYYY-MM-DD")
        
        # Get the dates based on start_date and end_date for a given frequency
        valuation_dates = self.get_dates(start_date, end_date, frequency)

        # Check if this request has been cached
        exists, filename = self.does_equity_curve_exist(basket, start_date, end_date, frequency)

        if exists :
            return pd.read_excel(SAVED_REQUESTS_DIRECTORY_PATH+ "/" + filename)

        # Initialize an empty DataFrame to store the results
        all_prices = pd.DataFrame()

        # Request pricing for each date
        for date in valuation_dates:
            prices = self.get_basket_prices(basket, date=date)
            prices['ValuationDate'] = date
            all_prices = pd.concat([all_prices, prices])

        # Save as file in the database
        all_prices.to_excel(SAVED_REQUESTS_DIRECTORY_PATH + "/" + filename, index=False)

        # Return the equity curve
        return all_prices
    

    def does_equity_curve_exist (self, basket : dict, start_date : str, end_date : str, frequency) :
        """
        
        Args:
            basket () : 
            start_date (str) :  Starting date, in format "YYYY-MM-DD"
            end_date (str) : Ending date, in format "YYYY-MM-DD"
            frequency (str) : Frequency for date like "Day", "Month", "Year", "Quarter"

        Returns:

        """
        filename = f"equity_curve_{basket['buySell']}_{basket['payoutCurrency']}_strike-{basket['strike']}_expi-{basket['expiryDate']}_from-{start_date}_to-{end_date}_each-{frequency}.xlsx"
        
        return filename in os.listdir(SAVED_REQUESTS_DIRECTORY_PATH), filename
    


