import os
from dotenv import load_dotenv

from lib.clients.generic_api import GenericApi

# GLOBAL INFORMATION VARIABLES

ICE_HOST=os.getenv("ICE_HOST")
ICE_AUTH=os.getenv("ICE_AUTH")

ICE_USERNAME=os.getenv("ICE_USERNAME")
ICE_PASSWORD=os.getenv("ICE_PASSWORD")

ICE_URL_SEARCH_TRADES=os.getenv("ICE_URL_SEARCH_TRADES")
ICE_URL_GET_TRADES=os.getenv("ICE_URL_GET_TRADES")
ICE_URL_CALC_RES=os.getenv("ICE_URL_CALC_RES")
ICE_URL_BIL_IM_CALC=os.getenv("ICE_URL_BIL_IM_CALC")
ICE_URL_TRADE_ADD=os.getenv("ICE_URL_TRADE_ADD")


class IceAPI (GenericApi) :

    def __init__ (self) :
        """
        Initialize the IceApi instance and authenticate with the ICE API.
        """
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)


    def get_trades_from_books (self, book_name : str) :
        """
        Get trade information from a specific book.

        Args :
            - book_name : str -> The name of the book.

        Returns:
            - list -> Information about the trades in the specified book.
        """
        trades = self.get(
            
            ICE_URL_SEARCH_TRADES,        
            body={

                "query" : {
                    "type" : "in",
                    "field" : "Book",
                    "values" : book_name
                }

            }

        )

        trade_ids = [trade['tradeLegId'] for trade in trades['tradeLegs']]
        infos = self.get_info_trade(trade_ids)

        return infos['tradeLegs'] if infos else None
        
    
    def get_info_trade (self, trade_ids : list) -> dict :
        """
        Get information about a specific trade list.

        Args :
            - trade_ids : list -> List of trade IDs.

        Returns :
            - response : dict -> Information about the specified trades.
        """
        response = self.get(

            ICE_URL_GET_TRADES,
            body={
                "tradeLegIds": trade_ids
            }

        )

        return response


    def get_calc_results (self, calculation_id) -> dict :
        """
        Get calculation results based on a specific calculation ID.

        Args :
            - calculation_id : str -> The ID of the calculation.

        Returns:
            - response : dict -> Calculation results
        """
        response = self.get(

            ICE_URL_CALC_RES,
            body={

                "calculationId" : calculation_id,
                "includeResultsInHomeCurrency" : "yes",
                "includeResultsInPortfolioCurrency" : "no"

            }

        )

        return response


    def run_im_bilateral (self, date : str, ctptys=True) -> dict :
        """
        Runs a bilateral IM calculation.

        Args :
            - date : str -> The date in format "YYYY-MM-DD"
            - ctptys : bool -> 

        Returns :
            - response : dict -> Result of the IM bilateral calculation.
        """
        verfied_date = date.strftime("%Y-%m-%d") if isinstance(date, str) else date

        body = {

            "valuation" : {
                "type" : "EOD",
                "date" : date
            },
            
            "bookNames": ["HV_CASH", "HV_EQUITY_OPTIONS", "HV_EXO_FX", "HV_FX/PM_OPTIONS"],
            "model": "SIMM"

        }

        if ctptys :
            body["counterParyNames"] = ["Goldman Sachs Group, Inc.", "MorganStanley", "European Depositary Bank", "Saxo Bank"]

        response = self.get(

            ICE_URL_BIL_IM_CALC,
            body=body

        )

        return response


    def get_post_im (self, date : str, counterparty_name : str) -> str :
        """
        Get post-IM information for a specific counterparty and date.

        Args :
            - date : str -> The date in the format "2020-09-30".
            - counterparty_name : str -> The name of the counterparty.

        Returns:
            - im : str -> Post-IM score.
        """
        im = "None"
        calculation_id = read_id_from_file(date, "IM")
        
        if not calculation_id :

            print('[*] Run claculation in ICE for date', date)
            calculation_id = self.run_im_bilateral(date)["calculationId"]

            write_to_file(date, calculation_id, "IM")
        
        calculation = self.get_calc_res(calculation_id)['results']
        
        for result in calculation :

            if result['group'] == counterparty_name :
                im = result['postIm']
        
        return im
    

    def post_cash_leg (self, currency : str, date : str, notional : float, counterparty : str, pay_receive : str) -> dict :
        """
        Post a cash leg trade.

        Args :
            - currency : str -> The currency of the cash leg.
            - date : str -> The delivery date in the format "YYYY-MM-DD".
            - notional : float -> The notional amount of the trade.
            - counterparty : str -> The counterparty involved in the trade.
            - pay_receive : str -> Specifies whether the trade is paying or receiving.

        Returns:
            - response : dict -> Result of the trade posting.
        """
        instrument = {

            "assetClass": "Cash",
            "currency": currency,
            "deliveryDate": date,
            "notional": notional,
            "payReceive": pay_receive,
            "sourceAssetClass": "FX"

        }

        settlement = {
            
            "currency" : currency,
            "type" : "CashSettled"

        }

        trades_legs = [

            {
                "counterparty" : counterparty,
                "customFields" : [],
                "discountingMode" : "LIBOR",
                "instrument" : instrument,
                "porfolioName" : "HV_PRE_TRADE",
                "settlement": settlement
            }

        ]

        trades = [

            {
                "description" : "IM - api",
                "tradeCode" : "IM",
                "tradeLegs" : trades_legs,
                "tradeName" : "IM GS test",
                "tradeType" : "IM GS test"
            }

        ]

        response = self.post(

            ICE_URL_TRADE_ADD,
            data={
                "trades" : trades
            }

        )

        return response


    def create_trade_trigger_fx (self, trade) :
        """
        Create a trigger FX trade.

        Args :
            - trade : dict -> Dictionary representing a trade. the trade should have the following keys
                {
                    'tradeCode': 'test-API',
                    'tradeName': 'test-API',
                    "buySell": "Buy", # or "Sell"
                    "baseCurrency": "USD",
                    "termCurrency": "CHF",
                    "notional": "35000",
                    "notionalCurrency": "USD",
                    "strike": "0.8498",
                    "callPut": "Call", # or "Put"
                    "expiryDate": "2025-11-20", # format "YYYY-MM-DD"
                    "Premium-currency": "EUR",
                    "Premium-Amount": 10,
                    "Premium-Markup": 0,
                    "Settlement-currency": "USD",
                    "Settlement-type": "Deliverable", # or "CashSettled"
                    "counterparty":  "Goldman Sachs Group, Inc.",  # or "MorganStanley", "European Depositary Bank", "Saxo Bank"
                }

        Returns :

        """