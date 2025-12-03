from __future__ import annotations

import time
import datetime as dt
from typing import Dict, Optional, List
from functools import lru_cache

from libapi.ice.client import Client
from libapi.config.parameters import (
    ICE_AUTH, ICE_HOST, ICE_USERNAME, ICE_PASSWORD,
    BOOK_NAMES_HV_LIST_SUBSET_N1, BOOK_NAMES_WR_LIST_ALL, BOOK_NAMES_HV_LIST_ALL,
    ICE_URL_BIL_IM_CALC, ICE_URL_INVOKE_CALC
)
from libapi.utils.calculations import *
from libapi.utils.results import *
from libapi.utils.formatter import date_to_str

class IceCalculator (Client) :
    """
    Client spécialisé ICE : lancements + récupération de résultats,
    avec cache mémoire et utilitaires factorisés.
    """


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

        status = super().authenticate(username, password)

        return status

    
    # -------------------------------------------------- IM Bilateral -------------------------------------------------- #


    def run_bilateral_im_calculation (
        
            self,
            date: Optional[str | dt.datetime] = None,
            fund : Optional[str] = None,
            ctptys : bool = True,
            endpoint : Optional[str] = None
        
        ) -> Optional[Dict]:
        """
        Launch a bilateral IM calculation (POST JSON) for differents counterparties and/or a given fundation.

        Args:
            date (str | datetime): Calculation date.
            fund (str): Fund type ("HV" or "WR").
            ctptys (bool): Whether to include a list of counterparties.

        Returns:
            dict: API response (usually includes "calculationId").
        """
        verified_date = date_to_str(date)
        fund = "HV" if fund is None else fund
        endpoint = ICE_URL_BIL_IM_CALC if endpoint is None else endpoint

        body = {

            "valuation" : {
            
                "type": "EOD",
                "date": verified_date  # Format YYYY-MM-DD (in string)
            
            },
            
            "bookNames" : BOOK_NAMES_HV_LIST_SUBSET_N1 if (fund == "HV" or ctptys) else BOOK_NAMES_WR_LIST_ALL,
            "model" : "SIMM",

        }

        if ctptys :
            body["counterPartyNames"] = ["GOLDMAN SACHS BANK EUROPE SE", "MORGAN STANLEY EUROPE SE", "European Depositary Bank", "SAXO BANK A/S", "UBS AG"]

        # Try with post (default GET)
        response = self.post(
            
            endpoint=endpoint,
            json=body

        )

        return response


    def get_post_im_by_ctpy (
            
            self,

            date : Optional[str | dt.date | dt.datetime] = None,
            fund : Optional[str] = None,
            type : Optional[str] = None,

            ctpy_name : Optional[str] = None,
        
        ) -> Optional[str] :
        """
        Get bilateral IM calculation results for all counterparties.

        Args:
            date (datetime): The date for which to run/retrieve the calculation.
            fund (str): The fund name.
            type (str): Calculation type label.

        Returns:
            list[dict] | None: List of result dictionaries for each counterparty.
        """
        date = date_to_str(date)
        ctpy_name = "MORGAN STANLEY EUROPE SE" if ctpy_name is None else ctpy_name
        
        type = "IM" if type is None else type
        fund = "HV" if fund is None else fund

        im = None
        start = time.time()

        calculation_id = read_id_from_file(date, fund, type) # Fund HV by default
        
        if calculation_id is None :

            print(f"\n[i] No calculation ID found. Running calculation in ICE for date {date}")
            
            calculation_dict = self.run_bilateral_im_calculation(date)
            calculation_id = calculation_dict.get("calculationId")

            write_to_file(calculation_id, date, type, fund)
            
        calculation = load_cache_results_from_id(calculation_id)

        if calculation is None :

            print(f"\n[*] Requesting ICE for calculations results {date}")

            calculation = self.get_calculation_results(calculation_id)
            save_cache_results(calculation_id, calculation)
        
        calc_res = calculation.get('results') if calculation is not None else None

        if calc_res is None :

            print("\n[-] Error during extraction: calculation result is not complete")
            return calc_res

        for result in calc_res :

            if result["group"] == ctpy_name :

                im = result["postIm"]
        
        print(f"\n[+] Find value for IM: {im}")
        print(f"\n[+] Retrieved Post-IM data in {time.time() - start:.2f} seconds")

        return im
    

    def get_bilateral_im_calculation_all_ctpy (
        
            self,

            date : Optional[str | dt.datetime | dt.date] = None,
            fund : Optional[str] = None,
            type : Optional[str] = None,
        
        ) -> Optional[List[Dict]] :
        """
        Get bilateral IM calculation results for all counterparties.

        Args:
            date (datetime): The date for which to run/retrieve the calculation.
            fund (str): The fund name.
            type (str): Calculation type label.

        Returns:
            list[dict] | None: List of result dictionaries for each counterparty.
        """
        date = date_to_str(date)
        fund = "HV" if fund is None else fund
        type = "IM" if type is None else type
        
        start = time.time()
        
        # First check and load a ID from "cache"
        calculation_id = read_id_from_file(date, fund, type) # Fund HV by default

        if not calculation_id :

            print(f"[*] Run calculation in ICE for date {date} \n")
            
            calculation_dict = self.run_bilateral_im_calculation(date, fund=fund)
            calculation_id = calculation_dict.get("calculationId")

            write_to_file(calculation_id, date, fund, type)

        calculation = load_cache_results_from_id(calculation_id)

        if calculation is None :
            
            print(f"\n[*] Requesting ICE for calculations results {date}")

            calculation = self.get_calculation_results(calculation_id)
            save_cache_results(calculation_id, calculation)
        
        calc_res = calculation.get('results') if calculation is not None else None

        if calc_res is None :

            print("[-] Error during fetching, calculation is None...")
            return None
        
        print(f"[+] Get Bilateral IM ctpy information in {time.time() - start} seconds")
        
        return calc_res
    

    def get_bilateral_im (
            
            self,
            
            date : Optional[str | dt.datetime | dt.date] = None,
            type : Optional[str] = None
        
        ) :
        """
        Get bilateral IM calculation at the portfolio level (no ctptys split).

        Args:
            date (datetime): The date of the calculation.
            type (str): Calculation type.

        Returns:
            list[dict] | None: Result of the bilateral IM calculation.
        """
        date = date_to_str(date)
        type = "IM-ptf" if type is None else type 

        start = time.time()
        
        # Check the cache first
        calculation_id = read_id_from_file(date, type=type) # Fund HV by default

        if not calculation_id :

            print(f"\n[*] Running ICE calculation for date {date}")
            
            calculation_dict = self.run_bilateral_im_calculation(date , ctptys=False)
            calculation_id = calculation_dict.get("calculationId")

            write_to_file(calculation_id, date, type=type)

        # Check if the results are already stored as cache        
        calculation = load_cache_results_from_id(calculation_id)

        if calculation is None :

            print(f"\n[*] Requesting ICE for calculations results {date}")

            calculation = self.get_calculation_results(calculation_id)
            save_cache_results(calculation_id, calculation)
        
        calc_res = calculation.get('results') if calculation is not None else None

        if calc_res is None :

            print("\n[-] Error: Calculation result is None")
            return None

        print(f"\n[+] Retrieved portfolio bilateral IM in {time.time() - start:.2f} seconds")
        
        return calc_res


    # -------------------------------------------------- MV and Greeks -------------------------------------------------- #


    def run_mv_n_greeks (self, date : Optional[str | dt.datetime] = None, book_names : Optional[List[str]] = None, endpoint : Optional[str] = None) -> Optional[dict] :
        """
        Trigger MV and Greeks calculation.

        Args:
            date (str | datetime): If provided, run for EOD date.

        Returns:
            dict: Response from the API.
        """
        endpoint = ICE_URL_INVOKE_CALC if endpoint is None else endpoint
        book_names = BOOK_NAMES_HV_LIST_ALL if book_names is None else book_names

        valuation = { "type" : "RealTime" } if date is None else { "type" : "EOD", "date" : date_to_str(date) } # In this case, date type (Null or Not) matters

        payload = {

            "valuation" : valuation,
            "bookNames" : book_names,
            "includeSubBooks" : "true"

        }

        response = self.post(

            endpoint=endpoint,
            data=payload

        )

        return response


    def get_mv_n_greeks (self, re_run : bool = False, type : str = "MV") -> Optional[Dict]:
        """
        Retrieve MV and Greeks, optionally re-running the calculation.

        Args:
            re_run (bool): Whether to re-run the calculation.
            type (str): Calculation type label.

        Returns:
            dict: Trade legs with MV and Greeks.
        """

        last_run_time, id_last = get_most_recent_calculation(type)

        current_time = dt.datetime.now()
        diff_time = current_time - last_run_time

        if re_run :

            print("\n[*] Time since last Market Value calculation:")
            print(f"\tElapsed: {diff_time}")
            print(f"\tLast Run: {last_run_time}")

            action = input("\n[?] Re-run calculation (y) or use existing result (n)? [y/n] ")

            if action.strip() in ["y", ""] :

                print("[*] Running new calculation...")

                results = self.run_mv_n_greeks()
                id_last = results.get("calculationId") if results is not None else None

                write_to_file(id_last, current_time, type)
            
            else :

                print("[*] Using previous calculation ID...")
        
        if id_last is None :

            print("[-] Error: No valid calculation ID found...")
            return None

        calc_results = self.get_calculation_results(id_last)
        trade_legs = calc_results.get("tradeLegs") if calc_results is not None else None

        return trade_legs
    

    def get_mv_n_greeks_from_last_run (self, re_run : bool = False, type : str = "MV") -> Optional[Dict] :
        """
        Fetch MV and Greeks from the last recorded run.

        Args:
            re_run (bool): Unused.
            type (str): Calculation type.

        Returns:
            list[dict] | None: Trade legs.
        """
        last_run_time, id_last = get_most_recent_calculation(type)

        current_time = dt.datetime.now()
        diff_time = current_time - last_run_time

        calc_results = self.get_calculation_results(id_last)
        trade_legs = calc_results.get("tradeLegs") if calc_results is not None else None

        return trade_legs
    

    def get_mv_n_greeks_daily (self, date : str | dt.datetime = dt.datetime.now()) -> Optional[Dict]:
        """
        Fetch MV and Greeks for a specific date, or run it if needed.

        Args:
            date (str | datetime): Date to retrieve the data for.

        Returns:
            dict: Trade legs from the calculation.
        """

        date = date_to_str(date)
        date_calc, calc_id = get_closest_date_calculation_by_type(date, type="MV")

        # No previous calculation
        if date_calc is None or calc_id is None :

            print("[-] No previous calculation found or unable to find closest date")
            return None
        
        id = None
        if date_calc != date :

            # Run the calculation for the given date
            print("[+] Running new MV and Greeks calculation...")

            mv_n_greeks_dict = self.run_mv_n_greeks()
            id = mv_n_greeks_dict.get("calculationId") if mv_n_greeks_dict is not None else None
        
        else : # The calculation has already been run for the given date, get the results
            
            print("[*] Using existing calculation ID...")
        
        if id is None :

            print("[-] Error: No valid calculation ID found")
            return None

        calc_results = self.get_calculation_results(id)
        trade_legs = calc_results.get("tradeLegs") if calc_results is not None else None

        return trade_legs


    def get_cash_ctpy (self, date : str | dt.datetime, fund : str = "HV", type : str = "MV") -> Optional[Dict] :
        """
        Fetch raw calculation data for a specific fund and date.

        Args:
            date (datetime): Calculation date.
            fund (str): Fund identifier.
            type (str): Calculation type.

        Returns:
            dict: Raw calculation result from ICE.
        """
        verified = date_to_str(date)

        date_formatted = verified.split(" ")[0] + " 00:00:00" #date.replace(hour=0, minute=0, second=0, microsecond=0)
        calculation_id = read_id_from_file(date_formatted, type, fund=fund)

        start = time.time()

        if not calculation_id :

            print(f"[*] Running ICE calculation for date {date_formatted}")
            
            calculation_dict = self.run_im_bilateral(date_formatted, fund=fund)
            calculation_id = calculation_dict.get("calculationId")

            write_to_file(calculation_id, date_formatted, "IM", fund=fund) # CHECK THIS LINE TODO

        calculation = self.get_calculation_results(calculation_id)
        
        return calculation


    def get_total_mv_data (self, date : str | dt.datetime, type : str = "MV") -> Optional[Dict] :
        """
        Retrieve total Market Value calculation for a given date.

        Args:
            date (datetime): Date for which MV is requested.
            type (str): Type of calculation.

        Returns:
            dict: Full MV calculation result.
        """
        verified_date = date_to_str(date)
        calculation_id = read_id_from_file(verified_date, type, timeSensitive=False)

        if not calculation_id : 

            print(f"[*] Running ICE MV calculation for date {verified_date}")

            calculation_dict = self.run_mv_n_greeks(verified_date)
            calculation_id = calculation_dict.get("calculationId")

            write_to_file(calculation_id, verified_date, type)

        calculation = self.get_calculation_results(calculation_id)

        return calculation
    
    # -------------------------------------------------- Cache -------------------------------------------------- #

    @lru_cache(maxsize=128)
    def _cached_calc_results (self, calculation_id : str | int) -> dict[str] :
        """
        Fetch and cache ICE calculation results to avoid repeated fetches.

        Args:
            calculation_id (str): Calculation ID from ICE.

        Returns:
            dict: Result dictionary.
        """
        return super().get_calculation_results(calculation_id)
    

    
