import os, sys
import pandas as pd

from datetime import datetime

from libApi.pricers.pricer import Pricer
from libApi.config.parameters import columnsInPricer, SAVED_REQUESTS_DIRECTORY_PATH, EQ_PRICER_CALC_PATH, EQ_PRICER_SOLVE_PATH

from libApi.instruments.instruments import (

    get_instruments_samestrike_sell_put_call_eq, 
    get_instruments_2_strikes_sell_put_call_eq, 
    get_instruments_sell_cs_eq, 
    get_instruments_sell_ps_eq, 
    get_instruments_put_eq, 
    get_instruments_call_eq,
    
)

strategies_instruments_creation = {

    'Straddle' : get_instruments_samestrike_sell_put_call_eq,
    'Strangle' : get_instruments_2_strikes_sell_put_call_eq,
    'Call Spread' : get_instruments_sell_cs_eq,
    'Put Spread' : get_instruments_sell_ps_eq,
    'Put' : get_instruments_put_eq,
    'Call' : get_instruments_call_eq,

}


class PricerEQ (Pricer) :


    def __init__ (self) -> None :
        super().__init__()


    def post_request_price (self, instruments : list[dict], date=datetime.now().strftime("%Y-%m-%d")) :
        """
        

        Args:
            instruments (list[dict]) : Array of dictionaries for instruments
            date (datetime) : Given date for the price , by default now()
            
        Returns:
            dt (DataFrame) : Results converted to JSON

        Example:
            instrument = [
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'},
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'}
            ]
        """
        # Set the id for each instrument
        for instr in instruments :

            if "ID" not in instr or instr['ID'] is None :
                instr['ID'] = instruments.index(instr)
    
        # Create instruments
        instruments_json = [

            self.create_json_for_instruments(

                instr['direction'], 
                instr['BBGTicker'],
                instr['opt_type'], 
                instr['strike'],
                instr['notional'],
                instr['expiry'],
                instr['ID'],
                instr['SettlementDate']

            ) for instr in instruments

        ]
        
        # Log the number of instruments in the request
        self.log_api_call(len(instruments))
        
        # Call the API
        json = self.api.post(
            
            EQ_PRICER_CALC_PATH, 
            data = {

                'valuation' : {
                    "type" : "EOD",
                    "Date" : date
                },

                'artifacts' : {

                    'instruments' : ['Spread', 'Theta'],

                    'underlyingAssets' : {
                        'EQ' : ['Delta', 'Gamma', "Vega", "MarketData"]
                    }

                }, 
                
                "Instruments" : instruments_json

            }
            
        )

        # print(json)
        # print(instruments_json)
        dt = self.treat_json_response_pricer(json, instruments)
        
        return dt 
    

    def get_opts_prices(self, instruments : list[dict], date=datetime.now().strftime("%Y-%m-%d")) :
        """
        
        Args:
            instruments (list[dict]) : Array of dictionaries for instruments
            date (datetime) : Given date for the price , by default now()
            
        Returns:
            dt (DataFrame) : Results converted to JSON

        Example:
            instrument = [
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'},
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'}
            ]
        """
        # Split instruments into smaller batches
        instrument_batches = self.split_list(instruments, 50)

        # Initialize an empty DataFrame to store the results
        all_prices = pd.DataFrame()

        # Request pricing for each batch of instruments
        for batch in (instrument_batches) : # For each batch, price the instruments
            
            prices_batch = self.post_request_price(batch, date=date)
            all_prices = pd.concat([all_prices, prices_batch])

        # Convert to numeric
        for col in columnsInPricer.keys():
            
            if columnsInPricer[col] == 'sum' and col in all_prices.columns :
                all_prices[col] = pd.to_numeric(all_prices[col].apply(lambda x: str(x).replace(",", "")), errors='coerce')
                
        return all_prices
    

    def price_strategy (self, strategy : str, assets : list, expiries : list, strikes: list, date=datetime.now().strftime("%Y-%m-%d"), details=False) :
        """
        prices a strategy for a given set of currencies, expiries and strikes

        Args:
            strategy (str) : 'Straddle', 'Strangle', 'Call Spread', 'Put Spread', 'Put', 'Call'
            assets (list) : list of currency pairs
            expiries (list) : list of expiry dates in format 'YYYY-MM-DD'
            strikes (list) : list of strikes (ATMF, ATM, or value, or any percentage itm or otm, or delta eg: 15itm, 20itmf, 20otm, or any percentage 100%, 120%, 90%)
            date (str) : Date of the price strategy
        
        Returns :
            all_prices_grouped (tuple) : sdfsdf

        Note:
            if itm or otm is used, keep in mind that this value will be set for every individual option in the stratergy
        """
        print(f"[*] Pricing {strategy} for {assets} with expiries {expiries} and strikes {strikes}")
        
        # Create all instruments to price
        instruments = strategies_instruments_creation[strategy](assets, expiries, strikes)
                
        # call the API
        all_prices = self.get_opts_prices(instruments, date=date)

        # Filter columns
        filtered_columns_in_pricer = {k: v for k, v in columnsInPricer.items() if k in all_prices.columns}
        
        # if only two columns, return the dataframes (this means request was unsuccessful)
        if len(all_prices.columns) == 2 :
            return all_prices, all_prices
        
        # Group by strategy
        all_prices_grouped = all_prices[

            list(filtered_columns_in_pricer.keys()) + ['stratid']
        
        ].groupby(['stratid']).agg(filtered_columns_in_pricer).reset_index()
        
        if details :
            return all_prices_grouped, all_prices
        
        # Return all prices
        return all_prices_grouped
    

    def create_json_for_instruments (self, direction : str, BBGTicker : str, opt_type : str, strike, notional, expiry : str, ID, SettlementDate : str) :
        """
        
        """
        payload = {

            "InstrumentType": "Vanilla",

            "UnderlyingAsset" : {
                "BBGTicker": BBGTicker
            },

            "BuySell" : direction,
            "CallPut" : opt_type,
            "Strike" : strike,
            "Notional" : notional,
            "ExpiryDate" : expiry,
            "SettlementDate" : SettlementDate,
            "Style" : "European",
            "ID": ID,
            #"PayoutCurrency":"EUR",

        }

        return payload
    

    def get_strike (self, BBG_ticker, opt_type, strike : str, expiry : str, valuation_date=datetime.now().strftime("%Y-%m-%d")) :
        """
        Function that takes an option and returns the strike based on a strike in the format of '100%'
        The function will return the strike based on the spot for the start date
        In order to find the strike. this function will first execute a pricing, and then call the solve method with the price obtained from the pricing
        so this function does 2 requests to the API

        Args:
            BBG_ticker (str): Bloomberg Ticker (eg: SX5E)
            opt_type (str): 'Call' or 'Put'
            strike (str): '100%', percentage
            expiry (str): expiry date in format 'YYYY-MM-DD'
            valuation_date (str) : Valuation date, by default now()

        """
        # First step is to execute the pricing in order to find the price of the given strategy
        # lets create the instruments
        instruments = strategies_instruments_creation[opt_type]([BBG_ticker], [expiry], [strike], direction="Buy")

        # lets call the API to price the strategy
        prices = self.get_opts_prices(instruments, date=valuation_date)
        
        # check if calculation was successful
        if len(prices.columns) == 2 :
            raise ValueError("[-] Pricing was not successful")

        ## lets get the price of the instrument
        MarketValueMid = prices['MarketValuePercent'].iloc[0] * 1000000 / 100
        # Get the volume
        volume = 1000000/prices['ReferenceSpot'].str.replace(",", "").astype(float).iloc[0]
        
        # Price currency
        currency = prices['notional_currency'].iloc[0]

        """
        try :
        
            print('Date:', valuation_date)
            print('price:', MarketValueMid)
            print('volume:', volume)
            print('currency:', currency)
            prices.to_excel('prices.xlsx')
        
        except :
        
            print('Could not save prices.xlsx')
        """

        # Now that we have the price, lets call the solve method
        res = self.solve_for_strike(BBG_ticker, "Buy", opt_type, expiry, MarketValueMid, volume, valuation_date, priceCurrency=currency)

        # Now that we have the strike, lets return it
        return res['instruments'][0]['solvedValue']
    

    def solve_for_strike(self, BBG_ticker, direction, opt_type, expiry, MarketValueMid, volume, valuation_date=datetime.now().strftime("%Y-%m-%d"), priceCurrency='EUR'):
        """

        Args:
            price (float): price of the option strategy (so for all of the options)
            date (str): date in format 'YYYY-MM-DD'(date of the valuation)
            instrument (list[dict]) :
        
        Example :
            instrument = [
            
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'},
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'}
            
            ]
        """

        # Log the number of instruments in the request
        self.log_api_call(1)

        payload = {

            "valuation" : {
            
                "type" : "EOD",
                "date" : valuation_date
            
            },

            "artifacts" : {
                "instruments" : ["Spread", "Theta", "Instrument"],
            },

            "solver" : {

                "TargetField" : "MarketValueMid",
                "TargetType" : "amount",
                "TargetValue" : MarketValueMid
            },

            "Instruments" : [

                {
                    "ID" : "1",
                    "InstrumentType" : "Vanilla",
                    
                    "UnderlyingAsset" : {
                        "BBGTicker": BBG_ticker
                    },

                    "BuySell" : direction,
                    #"StrikeDate" : valuation_date,
                    #"PremiumDate" : valuation_date,
                    "PayoutCurrency" : priceCurrency,
                    "notional_currency" : priceCurrency,
                    "CompoQuanto" : "compo",
                    "CallPut" : opt_type,
                    "Strike" : "{Solve}",
                    "volume" : volume,
                    #"Notional" : "1_000_000",
                    "ExpiryDate" : expiry,
                    "SettlementDate" : expiry,
                    "Style" : "European"
                }
            
            ]

        }
            
        response = self.api.post(EQ_PRICER_SOLVE_PATH, data=payload)

        return response
    

    def does_equity_curve_exist (self, direction : str, BBGTicker : str, opt_type : str, strike : str, notional : float, expiry : str, start_date : str, end_date : str, frequency='Day') :
        """
        Args:
            direction (str) : 'Buy' or 'Sell'
            BBGTicker (str): Bloomberg Ticker
            opt_type (str): 'Call' or 'Put'
            strike (str): '100%', percentage, or value of the strike (if percentage, it will be calculated based on the spot for the start date)
            notional (float): notional of the option
            expiry (str): expiry date in format 'YYYY-MM-DD'
            start_date (str): start date in format 'YYYY-MM-DD'
            end_date (str) : end date in format 'YYYY-MM-DD'
            frequency (str) : 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
        """
        filename = f"equity_curve_{direction}_{BBGTicker}_{opt_type}_{strike}_{notional}_expi-{expiry}_from-{start_date}_to-{end_date}_each-{frequency}.xlsx"
        
        return filename in os.listdir(SAVED_REQUESTS_DIRECTORY_PATH), filename


    def equity_curve (self, direction : str, BBGTicker : str, opt_type : str, strike : str, notional : float, expiry : str, start_date : str, end_date : str, frequency='Day') :
        """
        Args:
            direction (str) : 'Buy' or 'Sell'
            BBGTicker (str): Bloomberg Ticker
            opt_type (str): 'Call' or 'Put'
            strike (str): '100%', percentage, or value of the strike (if percentage, it will be calculated based on the spot for the start date)
            notional (float): notional of the option
            expiry (str): expiry date in format 'YYYY-MM-DD'
            start_date (str): start date in format 'YYYY-MM-DD'
            end_date (str) : end date in format 'YYYY-MM-DD'
            frequency (str) : 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
        """
        # Check if dates are in correct format
        if type(expiry) != str :
            raise KeyError('[-] Expiry is not a string')
        
        if type(start_date) != str:
            raise KeyError('[-] start_date is not a string')
        
        if type(end_date) != str:
            raise KeyError('[-] end_date is not a string')
        
        # Get the dates based on the start and end date and the frequency
        valuation_dates = self.get_dates(start_date, end_date, frequency)

        # First we need to check if this function call was already called or not
        exists, filename = self.does_equity_curve_exist(direction, BBGTicker, opt_type, strike, notional, expiry, start_date, end_date, frequency)
        
        if exists:
            return pd.read_excel(SAVED_REQUESTS_DIRECTORY_PATH + "/" + filename)

        # First thing to do is to get the strike of our option
        strike = self.get_strike(BBG_ticker=BBGTicker, opt_type=opt_type, strike=strike, expiry=expiry, valuation_date=start_date)

        # Now that we have the strike, lets create the instruments
        instruments = strategies_instruments_creation[opt_type]([BBGTicker], [expiry], [strike], direction=direction)
        
        # Initialize an empty DataFrame to store the results
        all_prices = pd.DataFrame()

        # Request pricing for each date
        for date in valuation_dates :

            prices = self.get_opts_prices(instruments, date=date)
            prices['ValuationDate'] = date
            
            all_prices = pd.concat([all_prices, prices])         
    
        # Save as file in the database
        all_prices.to_excel(SAVED_REQUESTS_DIRECTORY_PATH + "/" + filename, index=False)
        
        # return the equity curve
        return all_prices
    
"""
pricer = PricerEQ()

# Backtest strategy
all_prices = pricer.equity_curve(
    direction='Buy',
    BBGTicker='BNP FP',
    opt_type='Put', 
    strike='100%', 
    notional=1_000_000, 
    expiry='2023-12-23', 
    start_date='2023-01-01', 
    end_date='2023-12-23', 
    frequency='Month'
)
print(all_prices.describe)
print(all_prices)
"""