import os
import pandas as pd
from datetime import datetime

from libApi.config.parameters import columnsInPricer, SAVED_REQUESTS_DIRECTORY_PATH, EQ_PRICER_CALC_PATH
from libApi.pricers.pricer import Pricer

class PricerBasket (Pricer) :


    def __init__ (self,) -> None :
        super().__init__()

    
    def post_request_price (self, basket : dict, date=datetime.now().strftime("%Y-%m-%d")) -> pd.DataFrame :
        """
        
        """
        # Set the ID for the basket
        if "ID" not in basket or basket['ID'] is None :
            basket['ID'] = 1  # Assuming only one basket per request

        # Create the JSON structure for the basket
        basket_json = self.create_json_for_basket(basket)

        # Log the number of instruments
        self.log_api_call(1)

        # Create payload data for the API
        payload = {

            "valuation" : {

                "type" : "EOD",
                "Date" : date

            },

            "Artifacts" : {

                "instruments" : ['Spread', "Theta"],
                "UnderlyingAssets" : {
                    
                    'EQ' : ["Delta", "Gamma", "Vega", "MarketValue"]

                }

            },

            "Instruments" : [basket_json]

        }

        # Call the API
        response = self.api.post(

            EQ_PRICER_CALC_PATH,
            data=payload

        )

        
        response_df = self.treat_json_response_pricer(response, [basket])

        return response_df
    

    def get_basket_prices (self, basket : dict, date=datetime.now().strftime("%Y-%m-%d")) :
        """
        
        """
        # Call the API to price the basket
        prices = self.post_request_price(basket, date=date)

        # Convert to numeric
        for col in columnsInPricer.keys() :

            if columnsInPricer[col] == "Sum" and col in prices.columns : 
                prices[col] = pd.to_numeric(prices[col].apply(lambda x : str(x).replace(",", "")), errors='coerce')

        return prices
    

    def create_json_for_basket (self, basket : dict) -> dict :
        """
        
        """
        payload = {

            "InstrumentsType" : "Basket",
            "BuySell" : basket["buySell"],
            "CallPut" : basket["callPut"],
            "Strike" : basket["strike"],
            "Notional" : basket["notional"],
            "ExpiryDate" : basket["expiryDate"],
            "SettlementDate" : basket["settlementDate"],
            "PayoutCurrency" : basket["payoutCurrency"],
            "UnderlyingAssets" : basket["underlyingAssets"],
            "ID" : basket["ID"]
    
        }

        return payload
    

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
    


