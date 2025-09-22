import polars as pl
import datetime as dt
from typing import Optional, Dict, List

from libapi.config.parameters import (
    ICE_HOST, ICE_AUTH, ICE_USERNAME, ICE_PASSWORD, # ICE credentials
    ICE_URL_SEARCH_TRADES, ICE_URL_GET_TRADES, ICE_URL_TRADES_ADD, ICE_URL_GET_PORTFOLIOS, # Endpoints
    BANK_COUNTERPARTY_NAME, BOOK_NAMES_HV_LIST_SUBSET_N1, BOOK_NAMES_HV_LIST_ALL # Names (banks, books, etc)
)
from libapi.ice.client import Client
from libapi.utils.formatter import date_to_str


class TradeManager (Client) :


    def __init__ (
        
            self,
            ice_host : Optional[str] = None,
            ice_auth : Optional[str] = None,
            ice_username : Optional[str] = None,
            ice_password : Optional[str] = None,
            
        ) -> None :
        """
        Initialize the Trade Manager and authenticate against the ICE API.

        This sets up the base API host and authentication headers and performs
        login using the provided credentials.
        """
        ice_host = ICE_HOST if ice_host is None else ice_host
        ice_auth = ICE_AUTH if ice_auth is None else ice_auth
        
        ice_username = ICE_USERNAME if ice_username is None else ice_username
        ice_password = ICE_PASSWORD if ice_password is None else ice_password

        super().__init__(ice_host, ice_auth)
        self.authenticate(ice_username, ice_password)


    def authenticate (self, username : Optional[str] = None, password : Optional[str] = None) -> bool :
        """
        Proxy for the base Client.authenticate method.

        Args:
            username (str): ICE username.
            password (str): ICE password.

        Returns:
            bool: True if authentication was successful.
        """
        username = ICE_USERNAME if username is None else username
        password = ICE_PASSWORD if password is None else password

        return super().authenticate(username, password)


    # -------------------------------------------- Get request operations  --------------------------------------------  


    def get_trades_from_books (
            
            self,
            books : List[str] | str,
            type : str = "In",
            field : str = "Book",
            endpoint : Optional[str] = None,

        ) -> Optional[Dict] :
        """
        Returns all trades from selected books / Portfolios

        Args:
            books (List[str] | str) : Name of books / portfolios
            type (str) : Type of query, it refers to find any value that could match (cf. API doc)
            field (str) : type of field
            endpoint (str | None) : Endpoint for asking the informations

        Returns:
            response (Dict | None) : Big dictionnary with a trade list, status and requeestID 
        """
        if books is None :
            raise ValueError("[-] None name or void name for the book.")
        
        endpoint = ICE_URL_SEARCH_TRADES if endpoint is None else endpoint
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


    def get_trades_from_books_by_date (
            
            self,
            books : List[str] | str,
            dates : Optional[List[str | dt.datetime]] = None,
            query_type : str = "And",
            type : str = "In",
            field_trade : str = "TradeDate",
            field_book : str = "Book",
            endpoint : Optional[str] = None
        
        ) -> Optional[Dict] :
        """
        
        """
        dates = [date_to_str(date) for date in dates] if isinstance(dates, list) else [date_to_str(dates)]
        endpoint = ICE_URL_SEARCH_TRADES if endpoint is None else endpoint
        books = [books] if isinstance(books, str) else (None if not isinstance(books, list) else books)

        trade_date_query = {

            "type" : type,
            "field" : field_trade,
            "values" : dates

        }

        book_names_query = {

            "type" : type,
            "field" : field_book,
            "values" : books

        }

        payload = {

            "query" : {

                "type" : query_type,
                "queries" : [

                    trade_date_query,
                    book_names_query

                ]

            }
            
        }

        response = self.post(
            
            endpoint=endpoint,
            json=payload

        )

        return response


    def get_info_trades_from_ids (
        
            self,
            trade_ids : List,
            include_trade_fields : bool = True,
            endpoint : Optional[str] = None,
        
        ) -> Optional[Dict] :
        """
        Get information about specific trades.

        Parameters:
            trade_ids (list) : List of trade IDs.

        Returns:
            dict : Information about the specified trades.
        """
        endpoint = ICE_URL_GET_TRADES if endpoint is None else endpoint

        payload = {

            "includeTradeFields": include_trade_fields,
            "tradeLegIds": trade_ids

        }

        response : Dict = self.get(

            endpoint=endpoint,
            json=payload

        )

        # Format of the response
        # response : Dict = { "TradeLegs" : List[Dict[str, Any]] , "RequestId" : str , "status" : str }
        return response


    def get_info_trades_from_books (
            
            self,
            books : List[str] | str,
            type : str = "In",
            field : str = "Book",
            endpoint : Optional[str] = None

        ) -> Optional[Dict] :
        """
        This functions return all trades (in a dictionnary) from an specific book (in parameter)

        Args:
            name_book (str) : The name's book or Portfolio

        Returns:
            trades (list) : Information about the trades from the specific book
        """
        endpoint = ICE_URL_SEARCH_TRADES if endpoint is None else endpoint

        # Format of the response
        # response := { "TradeLegs" : List[Dict[str, Any]] , "RequestId" : str , "status" : str }
        response = self.get_trades_from_books(books, type, field, endpoint)

        if response is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_legs : List[Dict] = response.get("tradeLegs")
        trade_ids : List = [trade['tradeLegId'] for trade in trade_legs]

        # Format of the response
        # response : Dict = { "TradeLegs" : List[Dict[str, Any]] , "RequestId" : str , "status" : str }
        response : Optional[Dict] = self.get_info_trades_from_ids(trade_ids)

        return response


    def get_tickers_from_hv_equity_book (
            
            self,
            book : Optional[str] = None,
            type : str = "in",
            field : str = "Book",
            endpoint : Optional[str] = None,
        
        ) -> Optional[List] :
        """
        Retrieve a list of unique SD tickers associated with trades from a given HV Equity book.

        Args:
            book (str): The name of the book to search for trades. Defaults to the first in BOOK_NAMES_HV_LIST_SUBSET_N1.
            type (str): Query type to be passed to the API (e.g., "in"). Defaults to "in".
            field (str): Field to be used for filtering (e.g., "Book"). Defaults to "Book".
            endpoint (str): API endpoint for searching trades. Defaults to ICE_URL_SEARCH_TRADES.

        Returns:
            sdtickers (List[str] | None) : A list of unique SD tickers if found, or an empty list if none are available.
        """
        endpoint = ICE_URL_SEARCH_TRADES if endpoint is None else endpoint
        book = BOOK_NAMES_HV_LIST_SUBSET_N1[0] if book is None else book

        # Format of the response
        # response = List[Dict[str, Any]] where each Dict[str, Any] is a TradeLeg information
        response = self.get_trades_from_books(book, type, field,  endpoint)
        
        import time
        start = time.time()
        
        if response is None :
            raise RuntimeError("[-] Error during the GET request for trades")

        trade_legs = response.get("tradeLegs")
        trade_ids = [trade['tradeLegId'] for trade in trade_legs]

        infos = self.get_info_trades_from_ids(trade_ids)

        sdtickers = set()
        list_trades_lists : List[Dict] = infos.get("tradeLegs", [])

        for trade in list_trades_lists :
            
            instrument = trade.get("instrument", {})

            try :
                
                ticker = instrument.get("underlyingAsset", {}).get("sdTicker")

                if ticker :
                    sdtickers.add(ticker)

            except :

                continue # skip malformed or imcomplete entries
        
        print(f"[*] Operation done in {time.time() - start} seconds")

        return list(sdtickers)
    

    def get_all_existing_portfolios_raw (self, endpoint : Optional[str] = None) -> Optional[List[Dict]] :
        """
        Retrieve all existing portfolios in raw dictionary format.

        Args:
            endpoint (Optional[str], default=None): API endpoint to query. If None, uses the default ICE_URL_GET_PORTFOLIOS constant.

        Returns:
            Optional[List[Dict]]: A list of portfolio objects as dictionaries, each containing fields such as "portfolioName", "portfolioId", etc. 
        """
        endpoint = ICE_URL_GET_PORTFOLIOS if endpoint is None else endpoint

        # Format
        # response := { "portfolios" : List[Dict[str, Any]] , "requestId" : str , "status" : str }
        response = self.get(

            endpoint=endpoint,
            json={}

        )

        portfolios : List[Dict] = response.get("portfolios", [])

        return portfolios
    

    def get_all_existing_portfolio_names (self, endpoint : Optional[str] = None) -> Optional[List] :
        """
        Retrieve the names of all existing portfolios.

        Args:
            endpoint (Optional[str], default=None): API endpoint to query. If None, uses the default ICE_URL_GET_PORTFOLIOS constant.

        Returns:
            Optional[List[str]]: A list of portfolio names as strings.
        """
        return self.get_all_specific_portfolios_names(prefix=None, endpoint=endpoint)
    

    def get_all_existing_hv_portfolios (self, endpoint : Optional[str] = None) -> Optional[List] :
        """
        Retrieve all existing portfolios whose names start with 'HV'.

        Args:
            endpoint (Optional[str], default=None): API endpoint to query. If None, uses the default ICE_URL_GET_PORTFOLIOS constant.

        Returns:
            Optional[List[str]] : A list of portfolio names starting with 'HV'.
        """        
        return self.get_all_specific_portfolios_names("HV", endpoint=endpoint)
        

    def get_all_existing_wr_portfolios (self, endpoint : Optional[str] = None) -> Optional[List] :
        """
        Retrieve all existing portfolios whose names start with 'WR'.

        Args:
            endpoint (Optional[str], default=None): API endpoint to query. If None, uses the default ICE_URL_GET_PORTFOLIOS constant.

        Returns:
            Optional[List[str]] : A list of portfolio names starting with 'WR'.
        """
        return self.get_all_specific_portfolios_names("WR", endpoint=endpoint)
        

    def get_all_specific_portfolios_names (self, prefix : Optional[str] = "HV", endpoint : Optional[str] = None) :
        """
        Retrieve all existing portfolios whose names start with a given prefix.

        Args:
            prefix (Optional[str], default="HV"): String prefix to filter portfolio names. If None or empty string, returns all portfolio names.
            endpoint (Optional[str], default=None): API endpoint to query. If None, uses the default ICE_URL_GET_PORTFOLIOS constant.

        Returns:
            Optional[List[str]]: A list of portfolio names matching the prefix filter.
        """
        endpoint = ICE_URL_GET_PORTFOLIOS if endpoint is None else endpoint

        response = self.get_all_existing_portfolios_raw(endpoint)

        names = set()
        for portfolio in response :

            name = portfolio.get("portfolioName")

            if prefix is None or prefix == "" :
                names.add(name)
            
            else :
                
                formated_prefix = str(prefix).upper().strip()

                if str(name).startswith(formated_prefix) :
                    names.add(name)

        return list(names)


    # -------------------------------------------- Booking operations --------------------------------------------  TODO


    def post_cash_leg (
            
            self,
            currency : str,
            date : str | dt.datetime,
            notional : float,
            counterparty : str,
            pay_rec : str = "Pay",
            endpoint : Optional[str] = None
        
        ) :
        """
        
        """
        endpoint = ICE_URL_TRADES_ADD if endpoint is None else endpoint
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
            bank : Optional[str] = None,
            direction : str = "Pay",
            endpoint : Optional[str] = None

        ) :
        """
        
        """
        bank = BANK_COUNTERPARTY_NAME if bank is None else bank
        endpoint = ICE_URL_TRADES_ADD if endpoint is None else endpoint

        payload_bn = self.generate_cash_trade_payload(currency, date, bank if direction == "Pay" else counterparty, notional, book, pay_recv="Pay")
        payload_cp = self.generate_cash_trade_payload(currency, date, counterparty if direction == "Pay" else bank, notional, book)

        response = self.post(

            endpoint=endpoint,
            json={

                "trades" : [payload_bn, payload_cp]

            }

        )

        return response

    
    def post_trade_exotic_fx (self, trades : List, endpoint : str | None = None) :
        """
        Post a list of exotic FX trades (e.g., Knock-Out, Knock-In, Digital).
        
        Args:
            trades (List[dict]): A list of raw trade data dicts.
            endpoint (str): API endpoint to which trades will be posted.
        
        Returns:
            Response object from the API call.
        """
        endpoint = ICE_URL_TRADES_ADD if endpoint is None else endpoint

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


    # -------------------------------------------- Payload Generators --------------------------------------------  


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
            book : str | None = None,
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
        verfied_date = date_to_str(date)
        book =  BOOK_NAMES_HV_LIST_ALL[1] if book is None else book

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
    



    






