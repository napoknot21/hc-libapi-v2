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
        
        """
        response = self.get(

            

        )