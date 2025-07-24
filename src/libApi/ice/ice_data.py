from libApi.ice.generic_api import GenericApi
from libApi.config.parameters import *

class IceData (GenericApi) :

    def __init__ (self) -> None :
        """
        Initialize the IceData instance and authenticate with the ICE API.
        """ 
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)


    