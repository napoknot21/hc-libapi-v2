import requests, os
from datetime import datetime
from dotenv import load_dotenv

from urllib3.exceptions import InsecureRequestWarning

# Suppress only the InsecureRequestWarning from urllib3 needed for insecure connections
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

load_dotenv()
API_LOG_REQUEST_FILE_PATH=os.getenv("API_LOG_REQUEST_FILE_PATH")

class GenericApi :

    def __init__ (self, api_host : str, auth_url : str) :
        """
        Initializes the GenericAPI instance

        Args :
            - api_host : str -> The base URL of the API.
            - auth_url : str -> The authentication endpoint URL.

        Example :
            - api_host = "https://github.com"
            - auth_url = "/auth/login.php"
        
        """
        self.api_host = api_host
        self.auth_url = auth_url

        self.full_auth_url = f"{self.api_host}/{self.auth_url}"

        self.auth_token = None
        self.authenticated = False

        self.headers = {
            "Content-Type" : "application/json",
            "AuthenticationToken" : None
        }

        self.verify_ssl = False


    def authenticate (self, username : str, password : str) :
        """
        Authenticate the API using provided username and password.

        Args :
            - username : str -> The username for authentication.
            - passwprd : str -> The password for authentication.

        """
        auth_data = {
            "username" : username,
            "password" : password
        }

        try :

            response = requests.post(self.full_auth_url, headers=self.headers, json=auth_data, verify=self.verify_ssl)
            response.raise_for_status()

            self.auth_token = response.json().get('token')
            self.headers["AuthenticationToken"] = f"{self.auth_token}"
            self.authenticated = True

        except requests.exceptions.HTTPError as e :

            print(f"Authentication Error: {e.response.status_code} - {e.response.text}")

        except requests.exceptions.RequestException as e :

            print(f"Error during authentication: {e}")


    def get (self, endpoint : str, params=None, body=None) :
        """
        Make a GET request to the API.

        Args :
            - endpoint : str -> The API endpoint to make the GET request to.
            - params : dict, Optional -> Query parameters for the GET request.
            - body : dict, optional -> Request body for the GET request.

        Returns :
            - dict -> The API's JSON response
        """
        url = f"{self.api_host}/{endpoint}"

        try :

            response = requests.get(url, headers=self.headers, params=params, verify=self.verify_ssl, json=body)
            response.raise_for_status()

            print(f"[+] GET Request to {url} successful !")
            
            data = response.json()
            self.log_request("GET", url)

            return data

        except requests.exceptions.HTTPError as e :

            print(f"[-] GET Request Error: {e.response.status_code} - {e.response.text}")
        
        except requests.exceptions.RequestException as e :

            print(f"[-] Error making GET request: {e}")

    
    def post (self, endpoint : str, data : dict) :
        """
        Make a POST request to the API.

        Args :
            - endpoint : str -> The API endpoint to make the POST request to.
            - data : dict -> The data to be included in the POST request body.
        """
        url = f"{self.api_host}/{endpoint}"

        try :

            response = requests.get(url, headers=self.headers, verify=self.verify_ssl, json=data)
            response.raise_for_status()

            print(f"[+] POST Request to {url} successful !")
            
            data = response.json()
            self.log_request("GET", url)

            return data

        except requests.exceptions.HTTPError as e :

            print(f"[-] POST Request Error: {e.response.status_code} - {e.response.text}")
        
        except requests.exceptions.RequestException as e :

            print(f"[-] Error making POST request: {e}")


    def log_request (self, method, endpoint) :
        """
        Log the API request to a text file.

        Args :
            - method : str -> The HTTP method (GET or POST).
            - endpoint :str -> The API endpoint.
        """
        with open(API_LOG_REQUEST_FILE_PATH, "a") as log_file :

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write(f"[{timestamp}] {method} request to {endpoint}\n")
        