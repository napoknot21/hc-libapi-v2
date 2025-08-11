import os
import csv
import requests
import datetime as dt

from pathlib import Path
from typing import Dict, Any, Optional

from libapi.config.parameters import API_LOG_REQUEST_FILE_CSV_PATH, ICE_URL_CALC_RES, API_LOG_REQUEST_FILE_PATH

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
            token : str = None,
            is_auth : bool = False,
            verify_ssl : bool = False,
            timeout : int = 10

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

        self.session = self._build_session()


    def authenticate (self, username : str, password : str, url_endpoint : str = None) -> bool :
        """
        Authenticate with the API using username and password.

        Args:
            username (str): API username.
            password (str): API password.
            url_endpoint (str, optional): Alternative authentication endpoint.

        Returns:
            bool: True if authentication succeeds and a token is received, else False.
        """
        credentials = {

            "username" : username,
            "password" : password
        
        }

        full_endpoint = url_endpoint or self.full_auth_url

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

            self.token = json_response.get('token')
            
            if not self.token :
            
                print("[-] Auth succeeded but no token in the response...")
                return False

            # We have a non None token here
            self.headers["AuthenticationToken"] = self.token
            self.is_auth = True

            print("[+] API authentication successfully")
            
            return True

        except requests.exceptions.HTTPError as e :

            print(f"[-] Authentication Error: {e.response.status_code} - {e.response.text}\n")

        except requests.exceptions.RequestException as e :

            print(f"[-] Error during authentication: {e}\n")

        return False


    def get (self, endpoint : str, params : Dict = None) -> Optional[Dict[str, Any]] :
        """
        Send a GET request.

        Args:
            endpoint (str): API endpoint (relative path).
            params (dict, optional): Query parameters.

        Returns:
            dict | None: Parsed JSON response if successful, else None.
        """
        return self._make_request("GET", endpoint, params=params)

    
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
            log_abs_path : str = API_LOG_REQUEST_FILE_CSV_PATH
        
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


    def _build_session (
        
            self,
            retries: int = 3,
            backoff: float = 0.5,
            pool_connections: int = 15,
            pool_maxsize: int = 30
        
        ) -> requests.Session:
        """
        Create a configured requests.Session with retry and pooling.

        Args:
            retries (int): Number of retry attempts.
            backoff (float): Backoff multiplier between retries.
            pool_connections (int): Number of connection pools.
            pool_maxsize (int): Max connections per pool.

        Returns:
            requests.Session: Configured session object.
        """
        session = requests.Session()

        retry_cfg = Retry(
            
            total=retries,
            connect=retries,
            read=retries,
            status=retries,

            backoff_factor=backoff,
            
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"GET", "HEAD", "OPTIONS"},
            
            raise_on_status=False,
        
        )

        adapter = requests.HTTPAdapter(

            max_retries=retry_cfg,
            pool_connections=pool_connections,  # nb of pools by schema
            pool_maxsize=pool_maxsize,          # max connex by host
        
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Optional headers by default (ex: User-Agent custom)
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                # "User-Agent": "hc-libApi/2.0 (+https://tonprojet)"
            }
        )

        return session


    def _make_request (
            
            self,
            method : str,
            endpoint : str,
            params : Dict = None,
            data : Dict = None,
            json : Dict = None,
            headers : Dict = None,
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

            response.raise_for_status()
            
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


    def get_calc_results (self, calculation_id : str | int, endpoint_calc : str = ICE_URL_CALC_RES) -> Optional[Dict] :
        """
        Get calculation results by calculation ID.

        Args:
            calculation_id (str | int): ID of the calculation.
            endpoint_calc (str, optional): API endpoint for calculation results.

        Returns:
            dict | None: Calculation results if available.
        """
        response = self.get(

            endpoint=endpoint_calc,
            params={

                "calculationId" : calculation_id,
                "includeResultsInHomeCurrency" : "yes",
                "includeResultsInPortfolioCurrency" : "no"

            }

        )

        return response