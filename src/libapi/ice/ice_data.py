from libapi.ice.client import Client
from libapi.config.parameters import *


class IceData (Client) :

    def __init__ (self) -> None :
        """
        Initialize the IceData instance and authenticate with the ICE API.
        """ 
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)


    def volatility_surface (self) : 
        """
        
        """
        response = self.post(

            ICE_URL_QUERY_RESULTS,
            {
                "dataQueryId" : ICE_DATA_VS_ID
            }

        )

        return response
    
    
    def data_query (self) :
        """
        
        """
        response = self.post(

            ICE_URL_INVOKE_DQUERY,
            {
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