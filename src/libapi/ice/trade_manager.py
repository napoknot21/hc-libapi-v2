from typing import Optional, Dict, List

from libapi.ice.client import Client
from libapi.config.parameters import (
    ICE_HOST, ICE_AUTH, ICE_USERNAME, ICE_PASSWORD,
    ICE_URL_SEARCH_TRADES, ICE_URL_GET_TRADES, ICE_URL_TRADES_ADD, ICE_URL_GET_PORTFOLIOS,
    BOOK_NAMES_HV_LIST_SUBSET_N1
)


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

    
    def request_search_trades_from_books (
            
            self, books : List | str,
            endpoint : str = ICE_URL_SEARCH_TRADES,
            type : str = "in",
            field : str = "Book"

        ) -> Optional[Dict] :
        """
        
        """
        if books is None :
            raise ValueError("[-] None name or void name for the book.")
        
        books = [books] if isinstance(books, str) else books

        payload = {

            "query" : {
            
                "type" : type,
                "field" : field,
                "values" : books  
            
            }
        
        }

        response = self.post(

            endpoint=endpoint,
            json=payload

        )

        return response


    def get_info_trades_from_books (
            
            self,
            books : List | str,
            endpoint : str = ICE_URL_SEARCH_TRADES,
            type : str = "in",
            field : str = "Book"

        ) -> Optional[list] :
        """
        This functions return all trades (in a dictionnary) from an specific book (in parameter)

        Args:
            name_book (str) : The name's book or Portfolio

        Returns:
            trades (list) : Information about the trades from the specific book
        """
        response = self.request_search_trades_from_books(books, endpoint, type, field)

        if response is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_legs = response.get("tradeLegs")
        trade_ids = [trade['tradeLegId'] for trade in trade_legs]

        infos = self.get_info_trades_from_ids(trade_ids)
        infos_trades = infos["tradeLegs"] if infos and "tradeLegs" in infos else None

        return infos_trades
    

    def get_ticker_from_book_hv_equity (
            
            self,
            book : str = BOOK_NAMES_HV_LIST_SUBSET_N1[0],
            endpoint : str = ICE_URL_SEARCH_TRADES,
            type : str = "in",
            field : str = "Book"
        
        ) -> Optional[List] :
        """
        
        """
        response = self.request_search_trades_from_books(book, endpoint, type, field)

        if response is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_legs = response.get("tradeLegs")
        trade_ids = [trade['tradeLegId'] for trade in trade_legs]

        infos = self.get_info_trades_from_ids(trade_ids)

        sdtickers =  []

        for trade in infos["tradeLegs"] :

            try :
                
                ticker = trade.get("instrument", {}).get("underlyingAsset", {}).get("sdTicker")

                if ticker :
                    sdtickers.append(ticker)

            except :

                continue # skip amlformed entries

        return list(set(sdtickers))


    def get_info_trades_from_ids (self, trade_ids : List, endpoint : str = ICE_URL_GET_TRADES, include_trade_fields : bool = True) -> Optional[Dict]:
        """
        Get information about specific trades.

        Parameters:
        - trade_ids (list): List of trade IDs.

        Returns:
        - dict: Information about the specified trades.
        """
        payload = {

            "includeTradeFields": include_trade_fields,
            "tradeLegIds": trade_ids

        }

        response = self.get(

            endpoint=endpoint,
            json=payload

        )

        return response
    

    def post_cash_leg (self, currency, date, notional : float, couterparty : str, pay_rec : str, endpoint : str = ICE_URL_TRADES_ADD) :
        """
        
        """
        payload = {


        }

        response = self.post(

            endpoint=endpoint,
            data=payload

        )

        return response

    
    def get_all_books (self, endpoint : str = ICE_URL_GET_PORTFOLIOS) :
        """
        
        """
        response = self.get(

            endpoint=endpoint,
            json={}

        )

        portfolios = response.get("portfolios")

        return portfolios


    # TODO : FINISH REMAINING FUNCTIONS