
from libApi.ice.generic_api import GenericApi
from libApi.config.parameters import *


class TradeManager (GenericApi) :

    def __init__ (self) :
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)

    
    def get_trades_from_books (self, name_book : list) -> list :
        """
        This functions return all trades (in a dictionnary) from an specific book (in parameter)

        Args:
            name_book (str) : The name's book or Portfolio

        Returns:
            trades (list) : Information about the trades from the specific book
        """
        if name_book is None or name_book == "" :
            raise ValueError("[-] None name or void name for the book.")
        
        payload = {

            'query' : {

                "type" : "in",
                "field" : "Book",
                "values" : name_book
            
            }

        }

        trades = self.get(

            ICE_URL_SEARCH_TRADES,
            body=payload

        )

        if trades is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_ids = [trade['tradeLegId'] for trade in trades["tradeLegs"]]
        infos = self.get_info_trades(trade_ids)

        if infos and "tradeLegs" in infos :

            return infos["tradeLegs"]

        return []
    

    def get_ticker_from_book_hv_equity (self) :
        """
        
        """
        payload = {

            "query" : {
            
                "type" : "in",
                "field" : "Book",
                "values" : BOOK_NAMES[1] # Equity options book
            
            }
        
        }

        trades = self.get(

            ICE_URL_SEARCH_TRADES,
            body=payload

        )

        trade_ids = [trade["tradeLegId"] for trade in trades["tradeLegs"]]
        infos = self.get_info_trades(trade_ids)

        sdtickers =  []

        for trade in infos["tradeLegs"] :

            try :
                
                ticker = trade.get("instrument", {}).get("underlyingAsset", {}).get("sdTicker")

                if ticker :
                    sdtickers.append(ticker)

            except :

                continue # skip amlformed entries

        return list(set(sdtickers))


    def get_info_trades (self, trade_ids):
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

        return response
    
    
    