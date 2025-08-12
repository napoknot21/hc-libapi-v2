from typing import Optional, Dict

from libapi.ice.client import Client
from libapi.config.parameters import ICE_HOST, ICE_AUTH, ICE_USERNAME, ICE_PASSWORD, ICE_URL_SEARCH_TRADES, BOOK_NAMES_HV_LIST_SUBSET_N1, ICE_URL_GET_TRADES


class TradeManager (Client) :

    def __init__ (
        
            self,
            ice_host : str = ICE_HOST,
            ice_auth : str = ICE_AUTH,
            ice_username : str = ICE_USERNAME,
            ice_password : str = ICE_PASSWORD,
            
        ) -> None :
        """
        Initialize the Trade Manager and authenticate against the ICE API.

        This sets up the base API host and authentication headers and performs
        login using the provided credentials.
        """
        super().__init__(ice_host, ice_auth)
        self.authenticate(ice_username, ice_password)


    def authenticate (self, username : str = ICE_USERNAME, password : str = ICE_PASSWORD) -> bool :
        """
        Proxy for the base Client.authenticate method.

        Args:
            username (str): ICE username.
            password (str): ICE password.

        Returns:
            bool: True if authentication was successful.
        """
        return super().authenticate(username, password)

    
    def get_trades_from_books (self, name_book : list, endpoint_trade : str = ICE_URL_SEARCH_TRADES) -> Optional[list] :
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

            endpoint=endpoint_trade,
            json=payload

        )

        if trades is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_ids = [trade['tradeLegId'] for trade in trades["tradeLegs"]]
        infos = self.get_info_trades(trade_ids)

        if infos and "tradeLegs" in infos :

            return infos["tradeLegs"]

        return None
    

    def get_ticker_from_book_hv_equity (self, book_name : str = BOOK_NAMES_HV_LIST_SUBSET_N1[0], ticker_endpoint : str = ICE_URL_SEARCH_TRADES) :
        """
        
        """
        payload = {

            "query" : {
            
                "type" : "in",
                "field" : "Book",
                "values" : book_name # Equity options book
            
            }
        
        }

        trades = self.get(

            ticker_endpoint,
            json=payload

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


    def get_info_trades (self, trade_ids : list, trades_endpoint : str = ICE_URL_GET_TRADES):
        """
        Get information about specific trades.

        Parameters:
        - trade_ids (list): List of trade IDs.

        Returns:
        - dict: Information about the specified trades.
        """
        response = self.get(

            trades_endpoint,

            json={

                "includeTradeFields": True,
                "tradeLegIds": trade_ids

            }

        )

        return response
    
    
    # TODO : FINISH REMAINING FUNCTIONS