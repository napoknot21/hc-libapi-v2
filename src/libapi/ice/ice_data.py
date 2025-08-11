from typing import Optional, Dict, Any

from libapi.ice.client import Client
from libapi.config.parameters import *


class IceData (Client) :

    def __init__ (
            
            self,
            ice_host : str = ICE_HOST,
            ice_auth : str = ICE_AUTH,
            username : str = ICE_USERNAME,
            password : str = ICE_PASSWORD

        ) -> None :
        """
        Initialize the IceData instance and authenticate with the ICE API.
        """ 
        super().__init__(ice_host, ice_auth)
        self.authenticate(username, password)


    def volatility_surface (self, endpoint : str = ICE_URL_QUERY_RESULTS) -> Optional[Dict[Any, Any]]: 
        """
        
        """
        response = self.post(

            endpoint=endpoint,
            data={
                "dataQueryId" : ICE_DATA_VS_ID
            }

        )

        return response.json() if response is not None else None
    
    
    def data_query (self, endpoint : str = ICE_URL_INVOKE_DQUERY) -> Optional[Dict[Any, Any]]:
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

        return response.json() if response is not None else None