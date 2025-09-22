import tqdm
import pandas as pd
import polars as pl
import datetime as dt

from typing import Optional, Dict, List

from libapi.utils.formatter import *
from libapi.pricers.pricer import Pricer
from libapi.config.parameters import COLUMNS_IN_PRICER, FX_PRICER_SOLVE_PATH

from libapi.instruments.fx import *


strategies_instruments_creation = {

    'Straddle' : make_fx_straddle_payloads,
    'Strangle' : make_fx_strangle_payloads,
    'Call Spread' : make_fx_call_spread_payloads,
    'Put Spread' : make_fx_put_spread_payloads,
    'Put' : make_fx_put_leg_payloads,
    'Call' : make_fx_call_leg_payloads,

}


class PricerFX (Pricer) :

    def __init__ (self) :
        super().__init__()


    def request_fx_prices_api (
            
            self,
            instruments : List[Dict],
            time : Optional[str | dt.time] = None,
            date : Optional[str | dt.datetime] = None,
            underly_asset : str = "EURUSD",
            endpoint : str = FX_PRICER_SOLVE_PATH

        ) -> Optional[Dict] :
        """
        Request to the API, the price FX for different instruments

        Args:
            instruments (list[dict]) : Array of dictionaries for instruments
            date (str | dt.datetime) : Given date for the price , if is a str instance, should be "YYY-MM-DD" format
            endpoint (str) : Endpoint to request the information
            
        Returns:
            response (Dict) : Results converted to JSON

        Example:
            instrument = [
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'},
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'}
            ]
        """
        date = date_to_str(date)
        time = time_to_str(time)

        response = self.request_prices_api(instruments, "FX", time=time, date=date, underly_asset=underly_asset, endpoint=endpoint)

        return response#self.treat_json_response_pricer(response, instruments)


    def get_opts_prices (self, instruments : List, time :  str | dt.time, date : str | dt.datetime) :
        """
        
        """
        # Split instruments into smaller batches
        instrument_batches = self.split_list(instruments, 50)

        # Initialize an empty DataFrame to store the results
        all_prices = pd.DataFrame()

        # Request pricing for each batch of instruments
        for batch in tqdm.tqdm(instrument_batches) : 
            
            # For each batch, price the instruments
            prices_batch = self.post_request_price(batch, time, date=date)
            all_prices = pd.concat([all_prices, prices_batch])
        
        # Convert to numeric
        for col in COLUMNS_IN_PRICER.keys() :

            if COLUMNS_IN_PRICER[col] == 'sum' and col in all_prices.columns :
                all_prices[col] = pd.to_numeric(all_prices[col], errors='coerce')
                
        return all_prices
    

    def price_strategy (self, strategy, ccys : list, expiries : list, strikes, time="00:00", date=dt.datetime.now().strftime("%Y-%m-%d"), details=False) :
        """
        prices a straddle for a given set of currencies, expiries and strikes.

        Args:
            strategy (str) : strategy for the price, i.e. 'Put', 'Call', 'Straddle', 'Strangle', 'Call Spread', 'Put Spread'
            ccys (list) : List of currency pairs , i.e. ['EURUSD', 'EURCHF']
            expiries (list) : List of expiry dates in format 'YYYY-MM-DD', i.e ['2024-05-27']
            strikes (list): list of strikes, i.e. ['ATMF', 'ATM'] or value, or any percentage itm or otm, or delta eg: 15itm, 20itmf, 20otm)
        
        Note:
            if itm or otm is used, keep in mind that this value will be set for every individual option in the stratergy
        """
        print(f"[*] Pricing {strategy} for {ccys} with expiries {expiries} and strikes {strikes}\n")
        
        # Create all instruments to price
        instruments = strategies_instruments_creation[strategy](ccys, expiries, strikes)
        """
        Instruments is an list of dictionnaries, for each instrument, it is in the following format:
        => instrument = {
            'direction': 'Sell',
            'pair': 'EURUSD',
            'opt_type': 'Put',
            'strike': 'ATM',
            'notional': 1000000,
            'notional_currency': 'EUR',
            'expiry': '2026-07-22',
            'stratid': 360
        }
        """

        # call the API
        all_prices = self.get_opts_prices(instruments, time, date=date)  # This is a dataFrame
        
        # Filter columns
        filtered_columns_in_pricer = {k: v for k, v in COLUMNS_IN_PRICER.items() if k in all_prices.columns}
        
        # Group by stragegy
        all_prices_grouped = all_prices[
            list(filtered_columns_in_pricer.keys()) + ['stratid']
        ].groupby(['stratid']).agg(filtered_columns_in_pricer).reset_index()
        
        if details:
            return all_prices_grouped, all_prices
        
        # Return all prices
        return all_prices_grouped
    

    def create_json_for_instruments (self, direction, pair, opt_type, strike, notional, notional_currency, expiry, ID) :
        """
        
        """
        BaseCurrency = pair[:3]
        TermCurrency = pair[-3:]
        
        payload = {

            "InstrumentType" : "Vanilla",

            "UnderlyingAsset" : {
                "BaseCurrency" : BaseCurrency,
                "TermCurrency" : TermCurrency
            },
            
            "BuySell" : direction,
            "CallPut" : opt_type,
            "Strike" : strike,
            "Notional" : notional,
            "NotionalCurrency" : notional_currency,
            "ExpiryDate" : expiry,
            "Style" : "European",
            "ID": ID,

        }

        return payload


    def get_strike (self, strategy, ccys, expirires, strikes, time, valuation_date, solve = True) :
        """
        
        """
        # First step is to execute the pricing in order to find the price of the given strategy
        # lets create the instruments
        # instruments = strategies_instruments_creation[opt_type]([pair], [expiry], [strike], direction="Sell")
        _, opt = self.price_strategy(strategy, ccys, expirires, strikes, time, valuation_date, details=True)
        
    
        # check if calculation was successful
        if len(opt.columns) == 2 :
            raise ValueError("[-] Pricing was not successful")
            
        if solve == True :

            # Now that we have the price, lets call the solve method
            res = self.solve_for_strike(

                opt['pair'].iloc[0],
                opt['direction'].iloc[0],
                opt['opt_type'].iloc[0],
                opt['expiry'].iloc[0],
                opt['MarketPriceAskPercentBase'].iloc[0],
                time,
                valuation_date

            )
            
            # Now that we have the strike, lets return it
            return res, opt # ['instruments'][0]['solvedValue'], 
        
        else:

            return _, opt
        

    def solve_for_strike(self, pair, direction, opt_type, expiry, MarketPriceAskPercentBase, time, 
                         valuation_date=dt.datetime.now().strftime("%Y-%m-%d")) :
        """

        Args : 
            - price : float -> Price of the option strategy (so for all of the options)
            - date : str -> Sate in (a string) format 'YYYY-MM-DD' (date of the valuation)
        instrument (array of dicts)
        eg:
            [
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'},
                {'direction': 'Sell', 'BBGTicker': 'SX5E', 'opt_type': 'Call', 'strike': '100%', 'notional': 1000000, 'expiry': '2024-04-30', 'SettlementDate':'2024-05-02'}
            ]
        """
        
        # Log the number of instruments in the request
        self.log_api_call(1)

        temp = 1
        if direction == 'Sell' :
            temp = -1
        
        payload = {

            "valuation" : {

                "type": "Cut",
                "date": valuation_date,
                "time": time

            },

            "artifacts": {
                "instruments": ["Instrument"],
            },

            "solver": {

                "TargetField": "MarketPriceAsk",
                "TargetType": "amount",
                "TargetValue": temp * MarketPriceAskPercentBase * 1000000 / 100,
                "TargetValueCurrency": pair[:3]
            
            },

            "Instruments" : [
                {
                    "ID" : "1",
                    "InstrumentType": "Vanilla",
                    
                    "UnderlyingAsset": {
                        "BaseCurrency": pair[:3],
                        "TermCurrency": pair[-3:],
                    },

                    "BuySell" : direction,
                    "CityCut" : "NY 10:00 AM",
                    "CallPut" : opt_type,
                    "Strike" : "{Solve}",
                    "Notional" : 1_000_000,
                    "NotionalCurrency" : pair[:3],
                    "ExpiryDate" : expiry,
                    "Style" : "European"
                }
            ]

        }

        response = self.api.post(FX_PRICER_SOLVE_PATH, data=payload)

        return response