import datetime as dt
from typing import Optional, Dict

from libapi.ice.client import Client
from libapi.config.parameters import (
    ICE_AUTH, ICE_HOST, ICE_USERNAME, ICE_PASSWORD,
    ICE_DATA_EQ_TICKER_TENOR, ICE_URL_QUERY_RESULTS, ICE_URL_INVOKE_DQUERY
)
from libapi.utils.formatter import date_to_str, time_to_str


class IceData (Client) :


    def __init__ (
        
            self,
            ice_host : Optional[str] = None,
            ice_auth : Optional[str] = None,
            ice_username : Optional[str] = None,
            ice_password : Optional[str] = None,
            
        ) -> None :
        """
        Initialize the ICE calculator and authenticate against the ICE API.

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


    def fetch_volatility_surface (self, data_query : Optional[str] = None, endpoint : Optional[str] = None) -> Optional[Dict] :
        """
        Retrieve a (predefined) volatility surface by dataQueryId.

        Args:
            data_query_id: The ICE dataQueryId to fetch. Defaults to ICE_DATA_EQ_TICKER_TENOR.
            endpoint_url: Overrides the default results endpoint (ICE_URL_QUERY_RESULTS).

        Returns:
            Dict response payload from ICE.
        """
        endpont = ICE_URL_QUERY_RESULTS if endpont is None else endpont
        data_query = ICE_DATA_EQ_TICKER_TENOR if data_query is None else data_query

        response = self.post(

            endpoint=endpoint,
            data={

                "dataQueryId" : data_query

            }

        )

        return response
    
    
    def invoke_data_query (
        
            self,
            date : Optional[str | dt.datetime] = None,
            time : Optional[str | dt.time] = None,
            valuation_type : str = "Cut",
            time_zone : str =  "LND",
            ex_eod : bool = True,
            fields : Optional[str] = None,
            endpoint : Optional[str] = None
        
        ) -> Optional[Dict]:
        """
        Invoke a generic Data Query.

        Args:
            date: Valuation date (string or datetime/date). If None, formatter decides default.
            time: Valuation time (string or time). If None, formatter decides default.
            valuation_type: "Cut" (default), "PrevClose", etc., as supported by ICE.
            time_zone: e.g., "LND", "NYC".
            use_exchange_eod: Whether to use exchange EOD prices.
            fields: A field name or a sequence of field names. Defaults to ICE_DATA_EQ_TICKER_TENOR.
            assets: A sequence of asset identifiers. Defaults to ["MSFT"].
            endpoint_url: Overrides default invoke DQuery endpoint.

        Returns:
            Dict response payload from ICE.
        """
        fields = ICE_DATA_EQ_TICKER_TENOR if fields is None else fields
        endpoint = ICE_URL_INVOKE_DQUERY if endpoint is None else endpoint

        date = date_to_str(date)
        time = time_to_str(time)

        valuation = {

            "type" : valuation_type, 
            "date" : date,
            "timeZone" : time_zone,
            "useExchangeEOD" : ex_eod

        }

        data_query = {
                        
            "artifacts" : ["ValidateOnly"],
            "assets" : ["MSFT"],
            "dataType" : "string",

            "fields" : [fields],
            "valuation" : valuation

        }

        response = self.post(

            endpoint=endpoint,
            data={

                "dataQueries" : [data_query]

            }

        )

        return response
