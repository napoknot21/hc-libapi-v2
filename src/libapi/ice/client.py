import os
import csv
import json
import requests
import polars as pl
import datetime as dt

from pathlib import Path
from typing import Dict, Any, Optional, List

from libapi.config.parameters import (
    LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_CACHE_DIR_ABS_PATH,
    LIBAPI_CACHE_TOKEN_BASENAME, LIBAPI_LOGS_REQUESTS_BASENAME,
    ICE_URL_GET_CALC_RES, FREQUENCY_DATE_MAP
)
from libapi.utils.formatter import date_to_str

from urllib.parse import urljoin
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

# Suppress only the InsecureRequestWarning from urllib3 needed for insecure connections
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Client :

    def __init__ (
        
            self,
            api_host : str,
            auth_url : str,
            token : Optional[str] = None,
            is_auth : bool = False,
            verify_ssl : bool = False,
            timeout : int = 30,
            token_cache_path : Optional[str | Path] = "None"

        ) -> None :
        """
        Initialize the API client.

        Args:
            api_host (str): Base API URL (e.g., "https://example.com").
            auth_url (str): Authentication endpoint path.
            token (str, optional): Existing authentication token.
            is_auth (bool, optional): Force authentication state.
            verify_ssl (bool, optional): Verify TLS certificates (True in production).
            timeout (int, optional): Default timeout for requests in seconds.
        """
        self.api_host = api_host.rstrip("/")
        self.auth_url = auth_url.lstrip("/")

        self.full_auth_url = urljoin(self.api_host + "/", self.auth_url)

        self.token = token
        self.is_auth = bool(is_auth or token)

        self.headers = {

            "Content-Type" : "application/json",
            "AuthenticationToken" : token or None
        
        }

        self.verify_ssl = verify_ssl
        self.timeout = timeout

        self.session = requests.Session()

        TOKEN_CACHE_ABS_PATH = os.path.join(LIBAPI_CACHE_DIR_ABS_PATH, LIBAPI_CACHE_TOKEN_BASENAME)
        self.token_cache_path = TOKEN_CACHE_ABS_PATH if token_cache_path is None else str(token_cache_path)


    def authenticate (self, username : str, password : str, endpoint : Optional[str] = None) -> bool :
        """
        Authenticate with the API using username and password.

        Args:
            username (str): API username.
            password (str): API password.
            endpoint (str, optional): Alternative authentication endpoint.

        Returns:
            bool: True if authentication succeeds and a token is received, else False.
        """
        credentials = {

            "username" : username,
            "password" : password
        
        }

        full_endpoint = endpoint or self.full_auth_url

        self.token = self._load_cached_token()

        if self.token :

            # We have a token here
            self.headers["AuthenticationToken"] = self.token
            self.is_auth = True

            print("[+] Token and authentification loaded from cache")

            return self.is_auth

        try :

            response = self.session.post(

                url=full_endpoint,
                json=credentials,
                
                timeout=self.timeout,
                headers={k: v for k, v in self.headers.items() if v is not None},
                
                verify=self.verify_ssl,

            )

            response.raise_for_status()
            json_response = response.json()

            status = response.status_code

            self.token = json_response.get('token')
            
            if not self.token :
            
                print("[-] Auth succeeded but no token in the response...")
                return False

            self._save_token_to_cache(self.token)

            # We have a non None token here
            self.headers["AuthenticationToken"] = self.token
            self.is_auth = True

            print("[+] API authentication successfully")

        except requests.exceptions.HTTPError as e :
            
            status = e.status_code
            print(f"[-] Authentication Error: {e.response.status_code} - {e.response.text}\n")

        except requests.exceptions.RequestException as e :

            status = e.status_code
            print(f"[-] Error during authentication: {e}\n")

        finally :

            # Log at the end
            self.log_request(
                    
                    method="POST",
                    endpoint=full_endpoint,
                    status_code=status,
                    success=self.is_auth

                )

        return self.is_auth


    def get (self, endpoint : str, params : Dict = None, json : Dict = None) -> Optional[Dict[str, Any]] :
        """
        Send a GET request.

        Args:
            endpoint (str): API endpoint (relative path).
            params (dict, optional): Query parameters.

        Returns:
            dict | None: Parsed JSON response if successful, else None.
        """
        return self._make_request("GET", endpoint, params=params, json=json)

    
    def post (self, endpoint : str, data : Dict = None, json : Dict = None) -> Optional[Dict] :
        """
        Send a POST request.

        Args:
            endpoint (str): API endpoint (relative path).
            data (dict, optional): Form-encoded body data.
            json (dict, optional): JSON body.

        Returns:
            dict | None: Parsed JSON response if successful, else None.
        """
        return self._make_request("POST", endpoint, data=data, json=json)


    def log_request (
            
            self,
            method : str,
            endpoint : str,
            status_code : int = 404,
            success : bool = False,
            log_abs_path : Optional[str] = None
        
        ) -> None :
        """
        Log a request to a CSV file for tracking.

        Args:
            method (str): HTTP method.
            endpoint (str): API endpoint.
            status_code (int): HTTP status code.
            success (bool): Whether the request succeeded.
            log_abs_path (str): Absolute path to CSV log file.
        """
        REQUEST_ABS_PATH = os.path.join(LIBAPI_LOGS_DIR_ABS_PATH, LIBAPI_LOGS_REQUESTS_BASENAME)
        log_abs_path = REQUEST_ABS_PATH if log_abs_path is None else log_abs_path

        try:
            
            # Check if directory and file exists.
            path = Path(log_abs_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            new_file = not path.exists()

            with path.open(mode="a", newline="", encoding="utf-8") as csv_log_file :
                
                writer = csv.writer(csv_log_file)
                
                if new_file:
                    writer.writerow(["timestamp", "method", "endpoint", "status", "success"])

                timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([timestamp, method.upper(), endpoint, status_code or "", bool(success)])
                
                print(f"[*] [{timestamp}] {method} request to {endpoint}")
            
        except Exception as e :

            print("[-] Error while writing logs into the selected file.")


    def _make_request (
            
            self,
            method : str,
            endpoint : str,
            params : Optional[Dict] = None,
            data : Optional[Dict] = None,
            json : Optional[Dict] = None,
            headers : Optional[Dict] = None,
            timeout : int = 10
        
        ) -> Optional[Dict[str, Any]] :
        """
        Internal method to send HTTP requests.

        Args:
            method (str): HTTP method (GET, POST, etc.).
            endpoint (str): API endpoint (relative path).
            params (dict, optional): Query parameters.
            data (dict, optional): Form-encoded data.
            json (dict, optional): JSON payload.
            headers (dict, optional): Extra headers.
            timeout (int): Request timeout in seconds.

        Returns:
            dict | None: Parsed JSON if successful, else None.
        """
        # Cas incohérent: on pense être authentifié mais pas de token
        if self.is_auth and not self.token :

            print("[-] Auth state is True but no token is set.")
            return None
        
        # Normalize the endpoint/url
        endpoint_path = endpoint.lstrip("/")
        url = urljoin(self.api_host + "/", endpoint_path)

        # Effective headers (filter out None and merge extras)
        base_headers = {k: v for k, v in self.headers.items() if v is not None}

        if headers :
            base_headers.update({k: v for k, v in headers.items() if v is not None})

        success = False
        status = None
        response = None

        try :

            response = self.session.request(

                method=method.upper(),
                url=url,

                headers=base_headers,
                params=params,
                data=data,
                json=json,

                verify=self.verify_ssl,
                timeout=(timeout or self.timeout)

            )

            response.raise_for_status()

            success = True
            status = response.status_code
            
        except requests.exceptions.RequestException as e :
            
            status = getattr(getattr(e, "response", None), "status_code", None)

        
        finally :

            # Log at the end
            self.log_request(
                    
                    method=method,
                    endpoint=url,
                    status_code=status,
                    success=success

                )

        return response.json() if response is not None else None


    # -------------------------------------------------- Logic functions --------------------------------------------------


    def get_calculation_results (
        
            self,
            calculation_id : str | int,
            calculation_details : str = "Yes",
            results_home_ccy : str = "Yes",
            results_portf_ccy : str = "No",
            endpoint : Optional[str] = None

        ) -> Optional[Dict] :
        """
        Get calculation results by calculation ID.

        Args:
            calculation_id (str | int): ID of the calculation.
            endpoint_calc (str, optional): API endpoint for calculation results.

        Returns:
            dict | None: Calculation results if available.
        """
        endpoint = ICE_URL_GET_CALC_RES if endpoint is None else endpoint

        payload = {

            "calculationId" : str(calculation_id), # Send always in string format, even for integers
            "IncludeCalculationDetails" : calculation_details,
            "includeResultsInHomeCurrency" : results_home_ccy,
            "includeResultsInPortfolioCurrency" : results_portf_ccy

        }

        response = self.get(

            endpoint=endpoint,
            json=payload

        )

        return response
    

    # -------------------------------------------------- Auxiliar functions --------------------------------------------------


    def generate_dates (
            
            self,
            start_date : Optional[str | dt.datetime] = None,
            end_date : Optional[str | dt.datetime] = None,
            frequency : str = "Day",
            frequency_map : Optional[Dict] = None,
            format : str = "%Y-%m-%d"
        
        ) -> Optional[List]:
        """
        Function that returns a list of dates based on the start date, end date and frequency

        Args:
            start_date (str): start date in format 'YYYY-MM-DD'
            end_date (str): end date in format 'YYYY-MM-DD'
            frequency (str): 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
            
        Returns:
            list: list of dates in format 'YYYY-MM-DD' or None
        """
        start_date = date_to_str(start_date)
        end_date = date_to_str(end_date)

        start_date = dt.datetime.strptime(start_date, format)
        end_date = dt.datetime.strptime(end_date, format)

        frequency_map = FREQUENCY_DATE_MAP if frequency_map is None else frequency_map
        interval = frequency_map.get(frequency)

        if interval is None :

            print(f"[-] Invalid frequency: {frequency}. Choose from 'Day', 'Week', 'Month', 'Quarter', 'Year'.")
            return None

        # This return a Series
        try :
            series_dates = pl.date_range(start_date, end_date, interval=interval, eager=True)

        except Exception as e :
            
            print(f"[-] Error generating dates: {e}")
            return None

        if series_dates.len() == 0 :

            print("[-] Error during generation: empty range (check start & end).")
            return None
        
        # Filter out weekends for non-business day frequencies
        series_dates_wd = series_dates.filter(series_dates.dt.weekday() <= 5)
        
        if series_dates_wd.len() == 0 :

            print("[*] No week day in the generated list after filter. Returning an empty List")
            return []

        # Convert the date range to a list of strings in the format 'YYYY-MM-DD'
        range_date_list = (series_dates_wd
            .to_frame("dates")
            .with_columns(pl.col("dates").dt.strftime(format).alias("formatted_dates"))["formatted_dates"]
            .to_list()
        )

        print("[+] Successfully range date generated and converted to list")

        return range_date_list

    
    def _load_cached_token (self) -> Optional[str] :
        """
        
        """
        try :

            cache_path = os.path.abspath(self.token_cache_path)

            if os.path.exists(cache_path) :
                
                with open(self.token_cache_path, "r", encoding="utf-8") as f :

                    data = json.load(f)

                    token = data.get("token")
                    timestamp_str = data.get("timestamp")
                    expiration = data.get("expiration", 0)

                    if token and timestamp_str :

                        timestamp = dt.datetime.fromisoformat(timestamp_str)
                        age = (dt.datetime.now() - timestamp).total_seconds()
                        
                        if age < expiration :
                            return token
                        
        except Exception as e :
            
            print("[-] Error loading token from cache:", e)
        
        return None


    def _save_token_to_cache (self, token : Optional[str] = None, expiration : int = 3600) -> bool :
        """
        Auxiliar function for creating a cache file storing the token.
        Will create the cache directory if it does not exist.
        
        Args:
            token (str, optional): The token to save.
            expiration (int): Time in seconds before the token is considered expired.

        Returns:
            bool: True if the token was saved successfully, False otherwise.
        """
        status = False
        if token is None :

            print("[!] No token found into cache")
            return status

        try :

            data = {

                "token": token,
                "timestamp": dt.datetime.now().isoformat(),
                "expiration": expiration
            
            }

            # Ensure parent directory exists
            os.makedirs(os.path.dirname(self.token_cache_path), exist_ok=True)
        
            with open(self.token_cache_path, "w", encoding="utf-8") as f :
                json.dump(data, f)
            
            status = True

        except Exception as e :

            print("[-] Error saving token to cache: ", e)

        return status