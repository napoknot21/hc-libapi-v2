from typing import Optional, Dict, Any

from libapi.ice.client import Client
from libapi.config.parameters import *


class IceData (Client) :

    def __init__ (
        
            self,
            ice_host : str = ICE_HOST,
            ice_auth : str = ICE_AUTH,
            ice_username : str = ICE_USERNAME,
            ice_password : str = ICE_PASSWORD,
            
        ) -> None :
        """
        Initialize the ICE calculator and authenticate against the ICE API.

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


    def volatility_surface (self, endpoint : str = ICE_URL_QUERY_RESULTS) -> Optional[Dict]: 
        """
        
        """
        response = self.post(

            endpoint=endpoint,
            data={
                "dataQueryId" : ICE_DATA_VS_ID
            }

        )

        return response
    
    
    def data_query (self, endpoint : str = ICE_URL_INVOKE_DQUERY) -> Optional[Dict]:
        """
        
        """
        response = self.post(

            endpoint=endpoint,
            data={
                "dataQueries" : [
                    {
                        
                        "artifacts" : ["ValidateOnly"],
                        "assets" : ["MSFT"],
                        "dataType" : "string",
                        "fields" : [ICE_DATA_QUERY_ID],

                        "valuation" : {
                            "type": "Cut",
                            "date": "2024-06-03",
                            "time": "08:30",
                            "timeZone": "LND",
                            "useExchangeEOD": True
                        }

                    }
                ],
            }
        )

        return response