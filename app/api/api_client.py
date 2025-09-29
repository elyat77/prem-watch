# api_client.py
import requests
import time


class ApiClient:
    """Client for interacting with the football data API.

    Attributes:
        base_url (str): The base URL for the API.

    Methods:
        get_leagues(country_id=None): Fetches the list of leagues, optionally
            filtered by country ID.
    """

    def __init__(self, api_key: str) -> None:
        """
        Parameters:
            api_key (str): The API key for authenticating requests.
        """

        self.base_url = "https://api.football-data-api.com"
        self.__session = requests.Session()

        # Defaul params here
        self.__session.params = {"key": api_key}

        # Set headers if needed
        self.__session.headers.update({
            "Content-Type": "application/json"
        })

    def _make_request(self, method: str, endpoint: str, params: dict = None) -> dict:
        """Private helper method to construct and make API requests.

        Parameters:
            method (str): HTTP method (GET, POST, etc.).
            endpoint (str): API endpoint to hit.
            params (dict, optional): Query parameters for the request.
        """

        # Add a delay to avoid hitting rate limits of 1800/hour
        time.sleep(2)

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.__session.request(method, url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as err:
            print(f"Request error occurred: {err}")
        return None

    def get_leagues(self, country_id=None, chosen_only=True) -> dict:
        """Fetches the list of leagues from the football data API.
        Parameters:
            country_id (int, optional): Filter leagues by country ID
                (ISO number).
            chosen_only (bool, optional): If True, fetch only chosen
                leagues. Defaults to True.

        Returns:
            dict: The JSON response from the API containing league data.
        """

        params = {}
        if chosen_only:
            params["chosen_leagues_only"] = "true"
        if country_id:
            params["country"] = country_id

        return self._make_request("GET", "league-list", params)

    def get_countries(self) -> dict:
        """Fetches the JSON array of countries and associated ISO numbers.

        Returns:
            dict: The JSON response from the API containing country data.
        """

        return self._make_request("GET", "country-list")

    def get_matches(self, date=None) -> dict:
        """Fetches up to a maximum of 200 matches on the given day. Only works
        for selected leagues. If no date is provided, fetches today's matches.

        Parameters:
            date (str, optional): Date in the format 'YYYY-MM-DD'. Defaults to
                None which fetches today's matches.

        Returns:
            dict: The JSON response from the API containing the matches on the
                selected day.
        """

        params = {
            "timezone": "Europe/London"
        }
        if date:
            params["date"] = date
        return self._make_request("GET", "todays-matches", params)

    def get_league_stats(self, season_id, max_time=None) -> dict:
        """Fetches league statistics for a given season. If max_time is
        provided, fetches stats up to that time for the given league.

        Parameters:
            season_id (int): The ID of the season to fetch stats for.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched stats. The API will return the stats up to the time
                specified here. If none is entered, all stats are returned.
                Defaults to None.

        Returns:
            dict: The JSON response from the API containing league statistics.
        """

        params = {
            "season_id": season_id
        }
        if max_time:
            params["max_time"] = max_time
        return self._make_request("GET", "league-statistics", params)

    def get_schedule(self, season_id, max_time=None) -> dict:
        """Fetches the schedule/results for a given league season.

        Parameters:
            season_id (int): The ID of the season to fetch the schedule for.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched schedule. The API will return the schedule up to the
                time specified here. If none is entered, all schedule is
                returned. Defaults to None.

        Returns:
            dict: The JSON response from the API containing the league
                schedule.
        """

        params = {
            "season_id": season_id,
            "max_per_page": 1000
        }
        if max_time:
            params["max_time"] = max_time
        return self._make_request("GET", "league-matches", params)

    def get_league_teams(self, season_id, stats=False, max_time=None) -> dict:
        """Fetches the teams for a given league season. Offers detailed stats
        if requested.

        Parameters:
            season_id (int): The ID of the league season to fetch teams for.
            stats (bool, optional): If True, fetches detailed stats for each
                team. Defaults to False.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched stats. The API will return the stats up to the time
                specified here. If none is entered, all stats are returned.
                Defaults to None.

        Returns:
            dict: The JSON response from the API containing the teams in the
                league.
        """

        params = {
            "season_id": season_id,
        }
        if stats:
            params["include"] = "stats"
        if max_time:
            params["max_time"] = max_time
        return self._make_request("GET", "league-teams", params)

    def get_league_players(self, season_id, max_time=None) -> dict:
        """Fetches the players for a given league season.

        Parameters:
            season_id (int): The ID of the league season to fetch players for.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched stats. The API will return the stats up to the time
                specified here. If none is entered, all stats are returned.
                Defaults to None.

        Returns:
            dict: The JSON response from the API containing the players in the
                league.
        """

        params = {
            "season_id": season_id,
        }
        if max_time:
            params["max_time"] = max_time
        # Often returns results over multiple pages, so loop through all pages
        # and combine results
        res = self._make_request("GET", "league-players", params)
        if res and res["pager"]["max_page"] > 1:
            for page in range(2, res["pager"]["max_page"] + 1):
                params["page"] = page
                next_page = self._make_request("GET", "league-players", params)
                if next_page:
                    res["data"].extend(next_page["data"])
        return res

    def get_league_referees(self, season_id, max_time=None) -> dict:
        """Fetches the referees for a given league season.

        Parameters:
            season_id (int): The ID of the league season to fetch referees for.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched stats. The API will return the stats up to the time
                specified here. If none is entered, all stats are returned.
                Defaults to None.

        Returns:
            dict: The JSON response from the API containing the referees in
                the league.
        """

        params = {
            "season_id": season_id,
        }
        if max_time:
            params["max_time"] = max_time
        return self._make_request("GET", "league-referees", params)

    def get_team_data(self, team_id) -> dict:
        """Fetches detailed data for a given team.

        Parameters:
            team_id (int): The ID of the team to fetch data for.

        Returns:
            dict: The JSON response from the API containing detailed team data.
        """

        params = {
            "team_id": team_id,
        }
        return self._make_request("GET", "team", params)

    def get_team_recent_form(self, team_id) -> dict:
        """Fetches the last 5, 6, and 10 match stats for a given team. Will
        return all 3 stats with single call.

        Parameters:
            team_id (int): The ID of the team to fetch stats for.

        Returns:
            dict: The JSON response from the API containing the team's last
                5, 6, and 10 match stats.
        """

        params = {
            "team_id": team_id,
        }
        return self._make_request("GET", "lastx", params)

    def get_match_details(self, match_id) -> dict:
        """Fetches detailed data and trends for a single match. Includes
        general stats, H2H, and odds data.

        Parameters:
            match_id (int): The ID of the match to fetch data for.

        Returns:
            dict: The JSON response from the API containing detailed match
                data.
        """

        params = {
            "match_id": match_id,
        }
        return self._make_request("GET", "match", params)

    def get_league_table(self, season_id, max_time=None) -> dict:
        """Fetches the league table for a given league season.

        Parameters:
            season_id (int): The ID of the league season to fetch the table
                for.
            max_time (int, optional): Unix Timestamp for the upper limit of
                fetched stats. The API will return the stats up to the time
                specified here. If none is entered, all stats are returned.
                Defaults to None.

        Returns:
            dict: The JSON response from the API containing the league table.
        """

        params = {
            "season_id": season_id,
        }
        if max_time:
            params["max_time"] = max_time
        return self._make_request("GET", "league-tables", params)

    def get_player_stats(self, player_id) -> dict:
        """Fetches detailed stats for a given player across all seasons and
        leagues.

        Parameters:
            player_id (int): The ID of the player to fetch stats for.

        Returns:
            dict: The JSON response from the API containing detailed player
                stats.
        """

        params = {
            "player_id": player_id,
        }
        return self._make_request("GET", "player-stats", params)

    def get_referee_stats(self, referee_id) -> dict:
        """Fetches detailed stats for a given referee across all seasons and
        leagues.

        Parameters:
            referee_id (int): The ID of the referee to fetch stats for.

        Returns:
            dict: The JSON response from the API containing detailed referee
                stats.
        """

        params = {
            "referee_id": referee_id,
        }
        return self._make_request("GET", "referee", params)

    def get_btts_stats(self) -> dict:
        """Fetches the data of the best BTTS leagues, teams, and fixtures.

        Returns:
            dict: The JSON response from the API containing BTTS statistics.
        """

        return self._make_request("GET", "stats-data-btts")

    def get_over_25_stats(self) -> dict:
        """Fetches the data of the best Over 2.5 leagues, teams, and fixtures.

        Returns:
            dict: The JSON response from the API containing over/under
                statistics.
        """

        return self._make_request("GET", "stats-data-over25")
