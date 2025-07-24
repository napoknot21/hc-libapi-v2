import tqdm
import pandas as pd
from datetime import datetime

from libApi.pricers.pricer import Pricer
from libApi.config.parameters import columnsInPricer

from libApi.instruments.instruments import (

    get_instruments_samestrike_sell_put_call_fx, 
    get_instruments_2_strikes_sell_put_call_fx, 
    get_instruments_sell_cs_fx, 
    get_instruments_sell_ps_fx, 
    get_instruments_put_fx, 
    get_instruments_call_fx

)


strategies_instruments_creation = {

    'Straddle' : get_instruments_samestrike_sell_put_call_fx,
    'Strangle' : get_instruments_2_strikes_sell_put_call_fx,
    'Call Spread' : get_instruments_sell_cs_fx,
    'Put Spread' : get_instruments_sell_ps_fx,
    'Put' : get_instruments_put_fx,
    'Call' : get_instruments_call_fx,

}


class PricerFX (Pricer) :

    def __init__ (self) :
        super().__init__()


    def post_request_price(self, instruments, time, date=datetime.now().strftime("%Y-%m-%d")) :
        """
        
        """
        for instr in instruments :

            if "ID" not in instr or instr['ID'] is None :
                instr['ID'] = instruments.index(instr)

        # Log the number of instruments in the request
        self.log_api_call(len(instruments))

        # Call the API
        valuation = {

            "type" : "EOD",
            "Date" : date
        }

        if len(str(time)) != 0 :
            valuation["Time"] = time
        
        payload = {

            'valuation': valuation,
            'artifacts': {

                'instruments': ['Spread', 'Theta'],
                'underlyingAssets': {
                    'FX': ['Delta', 'Gamma', "Vega", "MarketData"]
                }

            },
            "Instruments": [

                self.create_json_for_instr(
                    instr['direction'], 
                    instr['pair'],
                    instr['opt_type'], 
                    instr['strike'],
                    instr['notional'],
                    instr['notional_currency'],
                    instr['expiry'],
                    instr['ID']
                ) for instr in instruments

            ]

        }

        json = self.api.post("/FX/api/v1/Calculate", data=payload)

        return self.treat_json_response_pricer(json, instruments)





    def get_strike (self, strategy, ccys, expirires, strikes, time, valuation_date, solve = True) :
        
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
        