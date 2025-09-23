import requests

class ApiClient:
    """
    Client for interacting with the football data API.
    """
    def __init__(self, api_key):
        self.base_url = "https://api.football-data-api.com"
        self.session = requests.Session()

        # Defaul params here
        self.session.params = {"key": api_key}

        # Set headers if needed
        self.session.headers.update({
            "Content-Type": "application/json"
        })

    def _make_request(self, method, endpoint, **kwargs):
        """ Private helper method to construct and make API requests. """
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as err:
            print(f"Request error occurred: {err}")
        return None

    def get_leagues(self, country_id=None):
        """Fetches the list of leagues from the football data API."""
        params = {}
        if country_id:
            params["country"] = country_id
        return self._make_request("GET", "league-list", params)
    


# API URL assembly constants
_BASE_URL = "https://api.football-data-api.com"
_GET_LEAGUES = f"{_BASE_URL}/league-list"

# Load API key from .env file
load_dotenv()
api_key = os.getenv("API_KEY")
key_param = {"key": api_key} # API key parameter for requests

#TODO: Add error handling for missing API key
#TODO: Add functions for making requests to the API

def get_leagues(chosen_only=True, country_id=None):
    """Fetches the list of leagues from the football data API."""
    response = requests.get(_GET_LEAGUES, params=key_param)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()   