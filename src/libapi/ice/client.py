import os, sys
import csv
import requests
import datetime as dt
import requests.adapters

from typing import Dict, Any, Optional

from libapi.config.parameters import API_LOG_REQUEST_FILE_CSV_PATH, ICE_URL_CALC_RES, API_LOG_REQUEST_FILE_PATH

from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

# Suppress only the InsecureRequestWarning from urllib3 needed for insecure connections
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Client :


    def __init__ (
        
            self,
            api_host : str,
            auth_url : str,
            token : Optional[str],
            is_auth : bool = False,
            verify_ssl : bool = False,
            timeout : int = 10

        ) -> None :
        """
        Initializes the API client.

        Args:
            api_host (str) : Base URL of the API, e.g. "https://github.com" (with or without trailing slash).
            auth_url (str) : Auth endpoint path, e.g. "/auth/login.php" (with or without leading slash).
            verify_ssl (bool) : Verify TLS certificates. Keep True in production.
            timeout (int) : Default request timeout (seconds).
            session: Optional preconfigured requests.Session.
        """
        self.api_host = api_host.rstrip("/") 
        self.auth_url = auth_url.lstrip("/")

        self.full_auth_url = f"{self.api_host}/{self.auth_url}"

        self.token = token
        self.is_auth = is_auth

        self.headers = {
            "Content-Type" : "application/json",
            "AuthenticationToken" : None
        }

        self.verify_ssl = verify_ssl
        self.timeout = timeout


    def authenticate (self, username : str, password : str, url_endpoint : Optional[str]) -> bool :
        """
        Authenticate the API using provided username and password.

        Args :
            - username : str -> The username for authentication.
            - passwprd : str -> The password for authentication.

        Returns:
            bool : True if authentication succeeds, False otherwise
        """
        credentials = {

            "username" : username,
            "password" : password
        
        }

        full_endpoint = url_endpoint if url_endpoint is not None else f"{self.api_host}/{self.auth_url}"

        try :

            response = requests.post(

                url=full_endpoint,
                json=credentials,

                timeout=self.timeout,
                headers=self.headers,
                verify=self.verify_ssl
                
            )

            response.raise_for_status()
            json_response = response.json() if response is not None else None

            self.token = json_response.get('token') if json_response is not None else None 
            self.headers["AuthenticationToken"] = f"{self.auth_token}"
            
            self.is_auth = True

            print("[+] API authentication successfully\n")

        except requests.exceptions.HTTPError as e :

            print(f"[-] Authentication Error: {e.response.status_code} - {e.response.text}\n")

        except requests.exceptions.RequestException as e :

            print(f"[-] Error during authentication: {e}\n")

        return self.is_auth


    def get (
            
            self,
            endpoint : str,
            params : Optional[Dict[str, Any]] = None,
            body : Optional[Dict[str, Any]] = None
            
        ) -> Optional[Dict[str, Any]] :
        """
        Send a GET request to the specified endpoint.

        Args:
            endpoint (str): Relative API endpoint (e.g., "user/profile").
            params (Optional[dict]): Query parameters for the request.

        Returns:
            Optional[dict]: Parsed JSON response if successful, otherwise None.
        """
        return self._make_request("GET", endpoint, params=params)


    
    def post (
        
            self,
            endpoint : str,
            data : Optional[Dict[str, Any]],
            json : Optional[Dict[str, Any]]

        ) -> Optional[Dict[str, Any]] :
        """
        Send a POST request to the specified endpoint.

        Args:
            endpoint (str): Relative API endpoint.
            data (Optional[dict]): Form data to send in the body (for `application/x-www-form-urlencoded`).
            json (Optional[dict]): JSON payload to send in the body.

        Returns:
            Optional[dict]: Parsed JSON response if successful, otherwise None.
        """
        return self._make_request("POST", endpoint, data=data, json=json)


    def log_request (
            
            self,
            method : str,
            endpoint : str,
            status_code : Optional[int],
            success : bool = False,
            log_abs_path : str = API_LOG_REQUEST_FILE_CSV_PATH
        
        ) -> None :
        """
        Logs details of each API request into a CSV file.

        Args:
            method (str): HTTP method used.
            endpoint (str): API endpoint.
            status_code (Optional[int]): HTTP status code.
            success (bool): Whether the request succeeded.
            log_abs_path (Optional[str]): Path to the log file. Defaults to FILE_ID_CALCULATION_CSV_PATH.
        """
        if log_abs_path is None or not os.path.isfile(log_abs_path) :
            # The path is incorrect or None was entered
            print("[-] No log file found or inorrect file path")
            return None

        try :

            with open(log_abs_path, mode="a", newline="", encoding="uft-8") as csv_log_file :

                writer = csv.writer(csv_log_file)

                timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                writer.writerow(
                    [
                        timestamp,
                        method.upper(),
                        endpoint,
                        status_code if status_code is not None else 404,
                        success
                    ]
                )

                print(f"[*] [{timestamp}] {method} request to {endpoint}\n")
            
        except Exception as e :

            print("[-] Error while writing logs into the selected file.")
            return



    def _build_session (self, retries: int = 3, backoff: float = 0.5) -> requests.Session :
        """
        
        """
        session = requests.Session()

        retry = Retry(

            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
            raise_on_status=False,
        
        )

        adapter = requests.adapters.HTTPAdapter(max_retries=retry)

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session
        

    def _make_request (
            
            self,
            method : str,
            endpoint : str,
            params : Optional[Dict[str, Any]],
            data : Optional[Dict[str, Any]],
            json : Optional[Dict[str, Any]],
            headers : Optional[Dict[str, Any]],
            timeout : int
        
        ) -> Optional[Dict[str, Any]] :
        """
        General request method for all HTTP verbs.

        Args:
            method (str): HTTP method, e.g., 'GET', 'POST'.
            endpoint (str): API endpoint (relative path).
            params (dict, optional): URL query parameters.
            data (dict, optional): Form-encoded data.
            json (dict, optional): JSON payload.
            headers (dict, optional): Extra headers to merge with base headers.

        Returns:
            dict | None: Parsed response JSON, or None on failure.
        """
        if not self.is_auth and (method.upper() != "POST" or method.upper() != "GET") :

            print("[-] Request attempted without authentication.")
            return None

        full_endpoint = f"{self.api_host}/{endpoint}"

        base_headers = self.headers.copy()

        if base_headers :
            # We add the headers parameter to the base headers
            base_headers.update(headers)

        sucess = False
        status = None

        try :

            response = requests.request(

                method=method.upper(),
                url=full_endpoint,

                headers=base_headers,
                params=params,
                data=data,
                json=json,

                verify=self.verify_ssl,
                timeout=(timeout if timeout is not None else self.timeout)

            )

            response.raise_for_status()

            sucess = True
            status = response.status_code
            
        except requests.exceptions.RequestException as e :
            
            status = e.response.status_code if hasattr(e, "response") and e.response else None

        # Log at the end
        self.log_request(
                
                method=method,
                endpoint=full_endpoint,
                status_code=status,
                success=sucess

            )

        return response.json() if response is not None else None

    # -------------------------------------------------- Logic functions ----------------------------------------------------

    def get_calc_results (self, calculation_id, endpoint_calc : str = ICE_URL_CALC_RES) -> Optional[Dict[Any, Any]] :
        """
        Get calculation results based on a specific calculation ID.

        Args:
            calculation_id (str) : The ID of the calculation.

        Returns:
            response (dict) : Calculation results
        """
        response = self.get(

            endpoint_calc,
            body={

                "calculationId" : calculation_id,
                "includeResultsInHomeCurrency" : "yes",
                "includeResultsInPortfolioCurrency" : "no"

            }

        )

        return response
    