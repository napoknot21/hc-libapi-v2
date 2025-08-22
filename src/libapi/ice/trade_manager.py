import datetime as dt
from typing import Optional, Dict, List

from libapi.ice.client import Client
from libapi.config.parameters import (
    ICE_HOST, ICE_AUTH, ICE_USERNAME, ICE_PASSWORD, BANK_COUNTERPARTY_NAME,
    ICE_URL_SEARCH_TRADES, ICE_URL_GET_TRADES, ICE_URL_TRADES_ADD, ICE_URL_GET_PORTFOLIOS,
    BOOK_NAMES_HV_LIST_SUBSET_N1, BOOK_NAMES_HV_LIST_ALL
)


def _as_date_str (date : str | dt.datetime = None) -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if date is None:
        date = dt.datetime.now()
    
    return date.strftime("%Y-%m-%d") if isinstance(date, dt.datetime) else str(date)


def _as_time_str (time : str | dt.time = None) -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if time is None :
        time = dt.datetime.now().time()

    return time.strftime("%H:%M:%S") if isinstance(time, dt.time) else str(time)


def _validate_date (date_str: str) -> Optional[dt.datetime] :
    """
    Validate `YYYY-MM-DD` and return a datetime or None.
    """
    try :

        return dt.datetime.strptime(date_str, "%Y-%m-%d")
    
    except Exception :
        
        return None

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
    

    def post_cash_leg (self, currency : str, date : str | dt.datetime, notional : float, counterparty : str, pay_rec : str = "Pay", endpoint : str = ICE_URL_TRADES_ADD) :
        """
        
        """
        payload = self.generate_cash_trade_payload(currency, date, counterparty, notional, pay_recv=pay_rec)

        response = self.post(

            endpoint=endpoint,
            data={
                "trades" : [payload]
            }

        )

        return response
    

    def post_margin_call (
            
            self,
            currency : str,
            date : str | dt.datetime,
            notional : float | int,
            book : str,
            counterparty : str,
            bank : str = BANK_COUNTERPARTY_NAME,
            direction : str = "Pay",
            endpoint : str = ICE_URL_TRADES_ADD

        ) :
        """
        
        """
        payload_bn = self.generate_cash_trade_payload(currency, date, bank if direction == "Pay" else counterparty, notional, book, pay_recv="Pay")
        payload_cp = self.generate_cash_trade_payload(currency, date, counterparty if direction == "Pay" else bank, notional, book)

        response = self.post(

            endpoint=endpoint,
            json={
                "trades" : [payload_bn, payload_cp]
            }

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


    def generate_trade_trigger_fx (self, trade : Dict) :
        """
        
        """
        payload = self.generate_cash_trade_payload(
            trade["c"]
        )


    def generate_cash_trade_payload (
            
            self,
            currency : str,
            date : str | dt.datetime,
            counterparty : str,
            notional : float = 1_000_000,
            book : str = BOOK_NAMES_HV_LIST_ALL[1],
            description : str = "IM - api",
            trade_code : str = "IM",
            trade_name : str = "IM",
            trade_type : str = "IMT",
            pay_recv : str = "Receive",
            asset_class :  str = "Cash",
            src_asset_class : str = "FX",
            settlement_type : str = "CashSettled",
            discount_mode : str = None,

        ) -> Optional[Dict] :
        """
        Generate a standardized trade payload for a simple cash trade (e.g. IM transfer).

        This function builds a trade dictionary with one trade leg, suitable for submission to
        an API or internal trade management system. Typically used for Initial Margin (IM)
        or simple FX/cash settlement flows.

        Args:
            currency (str): The currency of the trade and settlement (e.g., "EUR").
            date (str | datetime): Delivery date of the trade. Can be a string or datetime object.
            counterparty (str): The name of the trade counterparty.
            notional (float, optional): Notional amount of the trade. Defaults to 1,000,000.
            book (str, optional): Name of the portfolio or book to which the trade belongs.
            description (str, optional): Trade description. Defaults to "IM - api".
            trade_code (str, optional): Internal trade code. Defaults to "IM".
            trade_name (str, optional): Trade name. Defaults to "IM".
            trade_type (str, optional): Type of trade. Defaults to "IMT".
            pay_recv (str, optional): Indicates if this is a "Pay" or "Receive" trade. Defaults to "Receive".
            asset_class (str, optional): Asset class of the trade. Defaults to "Cash".
            src_asset_class (str, optional): Source asset class, typically for classification. Defaults to "FX".
            settlement_type (str, optional): Type of settlement (e.g., "CashSettled", "Deliverable"). Defaults to "CashSettled".
            discount_mode (str, optional): Discounting mode (e.g., "OIS"). Defaults to None.

        Returns:
            dict | None: A dictionary representing the trade payload, or None if generation fails.
        """
        verfied_date = _as_date_str(date)

        settlement = {

            "currency" : currency,
            "type" : settlement_type

        }

        instrument = {

            "assetClass" : asset_class,
            "currency" : currency,
            "deliveryDate" : verfied_date,
            "notional" : notional,
            "payReceive" : pay_recv,
            "sourceAssetClass" :  src_asset_class

        }

        trade_leg = {
            
            "instrument" : instrument,
            "customFields" : [],
            "portfolioName" : book,
            "settlement" : settlement,
            "counterparty" : counterparty

        }

        trade = {

            "description" : description,
            "tradeCode" : trade_code,
            "tradeLegs" : [trade_leg],
            "tradeName" : trade_name,
            "tradeType" : trade_type

        }

        if discount_mode is not None :
            trade["discountingMode"] = discount_mode

        return trade
    

    def post_trade_exo_fx (self, trades, endpoint : str = ICE_URL_TRADES_ADD) :
        """
        Post a list of exotic FX trades (e.g., Knock-Out, Knock-In, Digital).
        
        Args:
            trades (List[dict]): A list of raw trade data dicts.
            endpoint (str): API endpoint to which trades will be posted.
        
        Returns:
            Response object from the API call.
        """
        return self.post_trade(trades, self.create_trade_trigger_fx, endpoint)


    def post_trade (self, trades : List, creation_trade_function : callable, endpoint : str = ICE_URL_TRADES_ADD) :
        """
        GENERAL FUNCTION FOR SEND A POST TO THE ENDPOINT 
        """
        trades_list = []
        
        for trade in trades :

            trade_payload = creation_trade_function(trade)
            trades_list.append(trade_payload)

        response = self.post(

            endpoint=endpoint,
            json=trades_list

        )

        return response


    






