from __future__ import annotations

import time
import datetime as dt
from typing import Dict, Optional
from functools import lru_cache

from libapi.ice.client import Client

from libapi.config.parameters import ICE_AUTH, ICE_HOST, ICE_USERNAME, ICE_PASSWORD, BOOK_NAMES_HV_LIST_SUBSET_N1, BOOK_NAMES_WR_LIST_ALL, ICE_URL_BIL_IM_CALC, ICE_URL_INVOKE_CALC, BOOK_NAMES_HV_LIST_ALL
from libapi.utils.calculations import *


def _as_date_str (date : str | dt.datetime) -> str :
    """
    
    """
    return date.strftime("%Y-%m-%d") if isinstance(date, dt.datetime) else str(date)


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
        self.authenticate(ice_username, ice_password)


    def authenticate (self, username : str = ICE_USERNAME, password : str = ICE_PASSWORD) -> bool :
        """
        Proxy vers Client.authenticate.
        """
        return super().authenticate(username, password)

    
    # -------------------------------------------------- IM Bilateral --------------------------------------------------


    def run_im_bilateral(
        
            self,
            date: str | dt.datetime,
            fund : str = "HV",
            ctptys : bool = True,
            endpoint : str = ICE_URL_BIL_IM_CALC
        
        ) -> Optional[Dict]:
        """
        Lance un calcul IM bilatéral (POST JSON).

        Args:
            date: "YYYY-MM-DD" ou datetime.
            fund: "HV" ou "WR" (selon constantes BOOK_NAME_*).
            ctptys: inclure une liste de contreparties (sinon portefeuille global).

        Returns:
            dict: réponse API (incluant souvent "calculationId").
        """
        verified_date = _as_date_str(date)

        body = {

            "valuation" : {
            
                "type": "EOD",
                "date": verified_date
            
            },
            
            "bookNames" : BOOK_NAMES_HV_LIST_SUBSET_N1 if (fund == "HV" or ctptys) else BOOK_NAMES_WR_LIST_ALL,
            "model" : "SIMM",

        }

        if ctptys:
            body["counterPartyNames"] = ["Goldman Sachs Group, Inc.", "MorganStanley", "European Depositary Bank", "Saxo Bank"]

        # Try with post (default GET)
        response = self.post(
            
            endpoint=endpoint,
            json=body

        )

        return response


    def get_post_im (self, date : str | dt.datetime, counterparty_name : str = "MorganStanley", type : str = "IM") -> Optional[str] :
        """
        Get the post-IM information for a specific counterparty and date.

        Args :
            date (str | dt.datetime) : The date in the format "YYYY-MM-DD".
            counterparty_name (str) : The name of the counterparty.

        Returns:
            im (str) : Post-IM score.
        """
        im = None
        start = time.time()
        calculation_id = read_id_from_file(date, type)
        
        if calculation_id is None :

            print('[i] No calculation id found. Running calculation in ICE for date ', date)

            calculation_dict = self.run_im_bilateral(date)
            print(calculation_dict)
            
            calculation_id = calculation_dict.get("calculationId")
            #print(type(calculation_id))

            write_to_file(date, calculation_id, type)
        
        calculation = self.get_calc_results(calculation_id)
        calc_res = calculation.get('results') if calculation is not None else None

        if calc_res is None :

            print("[-] Error during fetching, calculation is None...")
            return calc_res

        for result in calc_res :

            if result['group'] == counterparty_name :
                im = result['postIm']
        
        print(f"[+] Get Post-IM information in {time.time() - start} seconds")
        return im
    

    def run_mv_n_greeks (self, date : str | dt.datetime = None, endpoint : str = ICE_URL_INVOKE_CALC) -> Optional[dict] :
        """
        Run MV and Greeks
        """
        valuation = { "type" : "RealTime" } if date is None else { "type" : "EOD", "date" : _as_date_str(date) }

        payload = {

            "valuation" : valuation,
            "bookNames" : BOOK_NAMES_HV_LIST_ALL,
            "includeSubBooks" : "true"

        }

        response = self.post(

            endpoint=endpoint,
            data=payload

        )

        return response


    def get_mv_n_greeks (self, ask_for_re_run : bool = False, type : str = "MV") -> Optional[Dict]:
        """
        Get MV and Greeks

        """
        last_run_time, id_last = get_last_run_time(type)

        current_time = dt.datetime.now()
        diff_time = current_time - last_run_time

        if ask_for_re_run :

            print("\n[*] Time elapse since last run of the Market Value request to the Ice Api\n")
            print('\t t = ' + str(diff_time))
            print("\t d = " + str(last_run_time))

            print('\n[?] Do you want to re-run this calculation (y) or use the previous id (n) ? [y/n] ')
            
            action = input()

            if action.strip() == "y" or action.strip() == "" :

                print('\n[*] Ran Calculation on API...\n')

                results = self.run_mv_n_greeks()
                id_last = results.get("calculationId") if results is not None else None

                write_to_file(current_time, id_last, type)
            
            else :

                print("\n[*] Using previous ID...\n")
        
        if id_last is None :

            print("[-] Error during fetching last ID from API...")
            return None

        calc_results = self.get_calc_results(id_last)
        trade_legs = calc_results.get("tradeLegs") if calc_results is not None else None

        return trade_legs
    

    def get_mv_n_greeks_from_last_run (self, ask_for_re_run : bool = False, type : str = "MV") :
        """
        
        """
        last_run_time, id_last = get_last_run_time(type)

        current_time = dt.datetime.now()
        diff_time = current_time - last_run_time

        calc_results = self.get_calc_results(id_last)
        trade_legs = calc_results.get("tradeLegs") if calc_results is not None else None

        return trade_legs
    

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
        date_formated = _as_date_str(date) 
        date.replace(hour=0, minute=0, second=0, microsecond=0)
        id_calc = read_id_from_file(date_formated, "IM-ptf")

        if not id_calc :

            print("[*] Run claculation in ICE for date ", date)
            
            calculation_id = self.run_im_bilateral(date, ctptys=False)['calculationId']
            print(calculation_id)
            write_to_file(date, calculation_id, "IM-ptf")
        
        calculation = self.get_calc_res(calculation_id)['results']
        
        return calculation


    def get_total_mv_data (self, date : str) -> Dict :
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
            response = self.run_mv_greeks(date=date[:10])

            calculation_id = response.get("calculationId")

            if calculation_id :
                write_to_file(date, calculation_id, "MV")


        calculation = self.get_calc_results(calculation_id) if calculation_id else {}

        return calculation
    

    # Cache mémoire pour éviter de re-fetcher un même id
    @lru_cache(maxsize=128)
    def _cached_calc_results (self, calculation_id : str) -> dict[str] :
        """
        
        """
        return super().get_calc_results(calculation_id)
    

    
