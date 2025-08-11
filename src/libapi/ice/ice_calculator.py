import datetime as dt

from __future__ import annotations
from functools import lru_cache

from libapi.ice.client import Client

from libapi.config.parameters import ICE_AUTH, ICE_HOST, ICE_USERNAME, ICE_PASSWORD
from libapi.utils.calculations import *


class IceCalculator (Client) :
    """
    Client spécialisé ICE : lancements + récupération de résultats,
    avec cache mémoire et utilitaires factorisés.
    """


    def __init__ (
        
            self,
            ice_host : str = ICE_HOST,
            ice_auth : str = ICE_AUTH,
            ice_username : str = ICE_USERNAME,
            ice_password : str = ICE_PASSWORD,

            auto_auth: bool = True,
            
        ) -> None :
        """
        Initialize the ICE calculator and authenticate against the ICE API.

        This constructor configures the base API host and authentication endpoint
        on the underlying client, then performs an authentication call using the
        provided credentials.
        
        On success, the authentication token is stored in
        the client's headers for subsequent requests.
        """
        super().__init__(ice_host, ice_auth)

        if auto_auth :

            if not ice_username or not ice_password :
                raise ValueError("[-] Missing ICE credentials\n")
            
            self.authenticate(ice_username, ice_password)


    def authenticate (self, username : str, password : str) :
        """
        
        """
        return super().authenticate(username, password)
    

    def run_im_bilateral (self, date : str, fund="HV", ctptys=True) -> dict :
        """
        Runs a bilateral IM calculation.

        Args :
            - date : str -> The date in format "YYYY-MM-DD"
            - ctptys : bool -> 

        Returns :
            - response : dict -> Result of the IM bilateral calculation.
        """
        verfied_date = date.strftime("%Y-%m-%d") if not isinstance(date, str) else date

        body = {

            "valuation" : {
                "type" : "EOD",
                "date" : verfied_date
            },
            
            "bookNames": BOOK_NAME_HV if fund == "HV" else BOOK_NAME_WR,
            "model": "SIMM"

        }

        if ctptys :
            body["counterParyNames"] = ["Goldman Sachs Group, Inc.", "MorganStanley", "European Depositary Bank", "Saxo Bank"]
            body["bookNames"] = BOOK_NAME_HV

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
    

    def run_mv_n_greeks (self, date=None) -> dict :
        """
        Run MV and Greeks
        """
        valuation = { "type" : "RealTime" } if not date else { "type" : "EOD", "date" : date }

        payload = {

            "valuation" : valuation,
            "bookNames" : BOOK_N_SUBBOOK_NAMES,
            "includeSubBooks" : "true"

        }

        response = self.post(

            ICE_URL_INVOKE_CALC,
            data=payload

        )

        return response


    def get_mv_n_greeks (self, ask_for_re_run=False) :
        """
        Get MV and Greeks

        """
        last_run_time, id_last = get_last_run_time("MV")

        current_time = datetime.now()
        diff_time = current_time - last_run_time

        if ask_for_re_run :

            print("\n[*] Time elapse since last run of the Market Value request to the Ice Api\n")
            print('\t t = ' + str(diff_time))
            print("\t d = " + str(last_run_time))

            print('\n[?] Do you want to re-run this calculation (y) or use the previous id (n) ? [y/n] ')
            
            action = input()

            if action == "y" or action == "" :

                print('\n[*] RAN CALCULATION ON API...\n')

                id_last = self.run_mv_n_greeks()["calculationId"]
                write_to_file(current_time, id_last, "MV")
            
            else :

                print("\n[*] USING PREVIOUS ID...\n")

        calc_results = self.get_calc_results(id_last)["tradeLegs"]

        return calc_results
    

    def get_mv_n_greeks_from_last_run (self, ask_for_re_run=False) :
        """
        
        """
        last_run_time, id_last = get_last_run_time("MV")

        current_time = datetime.now()
        diff_time = current_time - last_run_time

        calc_results = self.get_calc_results(id_last)["tradeLegs"]

        return calc_results
    

    def get_mv_n_greeks_daily (self, date_input) :
        """
        
        """
        date_calc, calc_id = get_closest_date_of_run_mv(datetime.strftime(date_input, "%Y-%m-%d"))

        # Si aucun calcul précédent n'existe
        if date_calc is None or calc_id is None:
            print('[-] No previous calculation found or ran calcuation on API\n')
            return None
        
        id = None 
        if date_calc[:10] != date_input :

            # Run the calculation for the given date
            print('\n[+] RAN CALCULATION ON API')
            id = self.runMvandGreeks()["calculationId"]
        
        else : # The calculation has already been run for the given date, get the results
            
            print('\n[*] USING PREVIOUS ID...\n')
        
        calc_results = self.get_calc_results(id)['tradeLegs']
        
        return calc_results


    def get_billateral_im_ctpy (self, date : dt.datetime, fund : str = "HV") :
        """
        
        """
        date_formated = date.replace(hour=0, minute=0, second=0, microsecond=0)
        id_calc = read_id_from_file(date_formated, "IM", fund=fund)

        if not id_calc :

            print(f"[*] Run calculation in ICE for date {date} \n")
            
            id_calc = self.run_im_bilateral(date_formated, fund=fund)["calculationId"]
            write_to_file(date_formated, id_calc, "IM", fund=fund)

        #print(id_calc)
        calculation = self.get_calc_results(id_calc)["results"]

        return calculation
    

    def get_cash_ctpy (self, date, fund="HV") :
        """
        Get billateral-IM calculation for a specific date

        Args:
            date (date): The date in the format "2020-09-30".

        Returns:
            calculation (json) : Result of the billateral-IM calculation.
        """
        date_formated = date.replace(hour=0, minute=0, second=0, microsecond=0)
        id_calc = read_id_from_file(date_formated, "MV", fund=fund)

        if not id_calc :

            print("[*] Run claculation in ICE for date ", date)
            
            id_calc = self.run_im_bilateral(date, fund=fund)["calculationId"]
            write_to_file(date, id_calc, "IM", fund=fund)

        #print(calculation_id)
        calculation = self.get_calc_results(id_calc)
        
        return calculation


    def get_billateral_im (self, date : str | dt.datetime = dt.datetime.now()) :
        """
        Get billateral-IM calculation for a specific date

        Args:
            date (date): The date in the format "2020-09-30".

        Returns:
            calculation (json) : Result of the billateral-IM calculation.
        """
        date_formated = _date_to_str(date) 
        date.replace(hour=0, minute=0, second=0, microsecond=0)
        id_calc = read_id_from_file(date_formated, "IM-ptf")

        if not id_calc :

            print("[*] Run claculation in ICE for date ", date)
            
            calculation_id = self.run_im_bilateral(date, ctptys=False)['calculationId']
            print(calculation_id)
            write_to_file(date, calculation_id, "IM-ptf")
        
        calculation = self.get_calc_res(calculation_id)['results']
        
        return calculation


    def get_total_mv_data (self, date : str) -> dict :
        """
        Get total market value for a specific date

        Args :
        - date : str -> The date in the format "2020-09-30 00:00:00".

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
    

    # Cache mémoire pour éviter de re-fetcher un même id
    @lru_cache(maxsize=128)
    def _cached_calc_results (self, calculation_id: str) -> dict[str] :
        return super().get_calc_results(calculation_id)
    

    
