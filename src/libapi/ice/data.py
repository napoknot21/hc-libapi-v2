import datetime as dt
from typing import Optional, Dict

from libapi.ice.client import Client
from libapi.config.parameters import *
from libapi.utils.formatter import date_to_str, time_to_str


class IceData (Client) :

    def __init__ (
        
            self,
            ice_host : Optional[str] = None,
            ice_auth : Optional[str] = None,
            ice_username : Optiona[str] = None,
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


    def authenticate (
        
            self,
            username : Optional[str] = None,
            password : Optional[str] = None
            
        ) -> bool :
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


    def volatility_surface (
            
            self,
            endpoint : Optional[str] = None
        
        ) -> Optional[Dict] :
        """
        
        """
        endpont = ICE_URL_QUERY_RESULTS if endpont is None else endpont

        response = self.post(

            endpoint=endpoint,
            data={

                "dataQueryId" : ICE_URL_DATA_VS_ID

            }

        )

        return response
    
    
    def data_query (
        
            self,
            date : Optional[str | dt.datetime] = None,
            time : Optional[str | dt.time] = None,
            valuation_type : str = "Cut",
            time_zone : str =  "LND",
            ex_eod : bool = True,
            endpoint : Optional[str] = None
        
        ) -> Optional[Dict]:
        """
        
        """
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
            "fields" : [ICE_DATA_QUERY_ID],
            "valuation" : valuation

        }

        response = self.post(

            endpoint=endpoint,
            data={

                "dataQueries" : [
                    data_query
                ],
            }

        )

        return response
    