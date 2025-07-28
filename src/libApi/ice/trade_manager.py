
from libApi.ice.generic_api import GenericApi
from libApi.config.parameters import (
    ICE_HOST, ICE_AUTH, ICE_USERNAME, ICE_PASSWORD, ICE_URL_SEARCH_TRADES, ICE_URL_GET_TRADES
)


class TradeManager (GenericApi) :

    def __init__ (self) :
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)

    
    def get_trades_from_book (self, name_book : str) -> list :
        """
        This functions return all trades (in a dictionnary) from an specific book (in parameter)

        Args:
            name_book (str) : The name's book or Portfolio

        Returns:
            trades (list) : Information about the trades from the specific book
        """
        if name_book is None or name_book == "" :
            raise ValueError("[-] None name or void name for the book.")
        
        query = {

            'query' : {

                "type" : "in",
                "field" : "Book",
                "values" : name_book
            
            }

        }

        trades = self.get(

            ICE_URL_SEARCH_TRADES,
            body=query

        )

        if trades is None :
            raise RuntimeError("[-] Error during the GET request for trades")
        """
        print(type(trades))
        print(trades)
        """
        trade_ids = [trade['tradeLegId'] for trade in trades["tradeLegs"]]
        infos = self.get_info_trade(trade_ids)

        if infos and "tradeLegs" in infos:
            print(infos["tradeLegs"])
            print()
            return infos["tradeLegs"]

        return []
    

    def get_info_trade(self, trade_ids):
        """
        Get information about specific trades.

        Parameters:
        - trade_ids (list): List of trade IDs.

        Returns:
        - dict: Information about the specified trades.
        """
        response = self.get(

            ICE_URL_GET_TRADES,
            body={
                "includeTradeFields": True,
                "tradeLegIds": trade_ids
            }

        )
        #print(response)
        return response