import polars as pl
import datetime as dt

from libapi.pricers.pricer import Pricer
from libapi.config.parameters import *
from libapi.instruments.instruments import *


class PricerDigital (Pricer) :

    def __init__ (self) -> None :
        super().__init__()


    def price_europeran_digital (
        
            self,
            direction : str,
            pair : str,
            strike : str,
            below_above : str,
            notional : float,
            expiry_date : str | dt.datetime = None,
            time : str | dt.time = "00:00",
            date : str | dt.datetime = None
        
        ) -> None :
        """
        
        """
        verfied_date = date or dt.datetime.now().strftime("%Y-%m-%d")
        
        instrument_dict = super().generate_payload_api(

            None,
            direction,
            "EuropeanDigital",
            strike,



        )

        return None