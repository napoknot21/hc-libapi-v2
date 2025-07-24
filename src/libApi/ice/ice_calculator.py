import os
from dotenv import load_dotenv

from libApi.ice.generic_api import GenericApi
from libApi.utils.calculations import *

# GLOBAL INFORMATION VARIABLES
load_dotenv()
ICE_HOST=os.getenv("ICE_HOST")
ICE_AUTH=os.getenv("ICE_AUTH")

ICE_USERNAME=os.getenv("ICE_USERNAME")
ICE_PASSWORD=os.getenv("ICE_PASSWORD")

ICE_URL_SEARCH_TRADES=os.getenv("ICE_URL_SEARCH_TRADES")
ICE_URL_GET_TRADES=os.getenv("ICE_URL_GET_TRADES")
ICE_URL_CALC_RES=os.getenv("ICE_URL_CALC_RES")
ICE_URL_BIL_IM_CALC=os.getenv("ICE_URL_BIL_IM_CALC")
ICE_URL_TRADE_ADD=os.getenv("ICE_URL_TRADE_ADD")


class IceCalculator (GenericApi) :

    def __init__ (self) -> None :
        """
        Initialize the IceApi instance and authenticate with the ICE API.
        """
        super().__init__(ICE_HOST, ICE_AUTH)
        self.authenticate(ICE_USERNAME, ICE_PASSWORD)

    
    def get_calc_results (self, calculation_id) -> dict :
        """
        Get calculation results based on a specific calculation ID.

        Args :
            - calculation_id : str -> The ID of the calculation.

        Returns:
            - response : dict -> Calculation results
        """
        response = self.get(

            ICE_URL_CALC_RES,
            body={

                "calculationId" : calculation_id,
                "includeResultsInHomeCurrency" : "yes",
                "includeResultsInPortfolioCurrency" : "no"

            }

        )

        return response
    

    def run_im_bilateral (self, date : str, ctptys=True) -> dict :
        """
        Runs a bilateral IM calculation.

        Args :
            - date : str -> The date in format "YYYY-MM-DD"
            - ctptys : bool -> 

        Returns :
            - response : dict -> Result of the IM bilateral calculation.
        """
        verfied_date = date.strftime("%Y-%m-%d") if isinstance(date, str) else date

        body = {

            "valuation" : {
                "type" : "EOD",
                "date" : date
            },
            
            "bookNames": ["HV_CASH", "HV_EQUITY_OPTIONS", "HV_EXO_FX", "HV_FX/PM_OPTIONS"],
            "model": "SIMM"

        }

        if ctptys :
            body["counterParyNames"] = ["Goldman Sachs Group, Inc.", "MorganStanley", "European Depositary Bank", "Saxo Bank"]

        response = self.get(

            ICE_URL_BIL_IM_CALC,
            body=body

        )

        return response


    def get_post_im (self, date : str, counterparty_name : str) -> str :
        """
        Get post-IM information for a specific counterparty and date.

        Args :
            - date : str -> The date in the format "2020-09-30".
            - counterparty_name : str -> The name of the counterparty.

        Returns:
            - im : str -> Post-IM score.
        """
        im = "None"
        calculation_id = read_id_from_file(date, "IM")
        
        if not calculation_id :

            print('[*] Run claculation in ICE for date', date)
            calculation_id = self.run_im_bilateral(date)["calculationId"]

            write_to_file(date, calculation_id, "IM")
        
        calculation = self.get_calc_res(calculation_id)['results']
        
        for result in calculation :

            if result['group'] == counterparty_name :
                im = result['postIm']
        
        return im
    

    def get_total_mv_data (self, date) -> dict :
        """
        Get total market value for a specific date

        Args :
        - date : date -> The date in the format "2020-09-30 00:00:00".

        Returns:
        - calculation : dict -> Result of the total market value calculation.
        """
        calculation_id = read_id_from_file(date, "MV", timeSensitive=False)

        if not calculation_id : 

            print("[*] Run calculation in ICE for date", date)
            calculation_id = self.run_mv_greeks(date=date[:-9])["calculationId"]

            write_to_file(date, calculation_id, "MV")

        calculation = self.get_calc_results(calculation_id)

        return calculation
