# test_api_client.py
"""
Unit tests for the ApiClient class in api_client.py.
Uses pytest and unittest.mock to test various methods of the ApiClient.
"""
import pytest
from unittest.mock import patch, Mock
from app.api.api_client import ApiClient
import requests

@pytest.fixture
def api_client():
    return ApiClient("test_api_key")

@patch('app.api.api_client.requests.get')
def test_get_leagues(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"leagues": [{"id": 1, "name": "Premier League"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    leagues = api_client.get_leagues()
    assert leagues == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_get_matches(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"matches": [{"id": 101, "homeTeam": "Team A", "awayTeam": "Team B"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    matches = api_client.get_matches(league_id=1)
    assert matches == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/matches",
        headers={"X-Auth-Token": "test_api_key"},
        params={"league": 1}
    )

@patch('app.api.api_client.requests.get')
def test_get_teams(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"teams": [{"id": 201, "name": "Team A"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    teams = api_client.get_teams(league_id=1)
    assert teams == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/teams",
        headers={"X-Auth-Token": "test_api_key"},
        params={"league": 1}
    )

@patch('app.api.api_client.requests.get')
def test_get_team_players(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"players": [{"id": 301, "name": "Player A"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    players = api_client.get_team_players(team_id=201)
    assert players == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/teams/201/players",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_get_player_stats(mock_get, api_client):    
    mock_response = Mock()
    expected_data = {"stats": {"goals": 10, "assists": 5}}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    stats = api_client.get_player_stats(player_id=301)
    assert stats == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/players/301/stats",
        headers={"X-Auth-Token": "test_api_key"}
    )
@patch('app.api.api_client.requests.get')
def test_rate_limiting(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"leagues": [{"id": 1, "name": "Premier League"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    # Call the method multiple times to trigger rate limiting
    for _ in range(3):
        leagues = api_client.get_leagues()
        assert leagues == expected_data

    assert mock_get.call_count == 3

@patch('app.api.api_client.requests.get')
def test_handle_api_error(mock_get, api_client):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.json.return_value = {"error": "Internal Server Error"}
    mock_get.return_value = mock_response

    with pytest.raises(Exception) as excinfo:
        api_client.get_leagues()
    
    assert "API request failed with status code 500" in str(excinfo.value)
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_handle_network_error(mock_get, api_client):
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    with pytest.raises(Exception) as excinfo:
        api_client.get_leagues()
    
    assert "Network error occurred: Network error" in str(excinfo.value)
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_empty_response(mock_get, api_client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}
    mock_get.return_value = mock_response

    leagues = api_client.get_leagues()
    assert leagues == {}
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_invalid_json_response(mock_get, api_client):    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_response

    with pytest.raises(Exception) as excinfo:
        api_client.get_leagues()
    
    assert "Failed to parse JSON response: Invalid JSON" in str(excinfo.value)
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_timeout_error(mock_get, api_client):
    mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

    with pytest.raises(Exception) as excinfo:
        api_client.get_leagues()
    
    assert "Network error occurred: Request timed out" in str(excinfo.value)
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_get_matches_with_date_range(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"matches": [{"id": 102, "homeTeam": "Team C", "awayTeam": "Team D"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    matches = api_client.get_matches(league_id=1, date_from="2023-01-01", date_to="2023-01-31")
    assert matches == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/matches",
        headers={"X-Auth-Token": "test_api_key"},
        params={"league": 1, "dateFrom": "2023-01-01", "dateTo": "2023-01-31"}
    )

@patch('app.api.api_client.requests.get')
def test_get_teams_no_league_id(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"teams": [{"id": 202, "name": "Team B"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    teams = api_client.get_teams()
    assert teams == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/teams",
        headers={"X-Auth-Token": "test_api_key"},
        params={}
    )

@patch('app.api.api_client.requests.get')
def test_get_team_players_invalid_team_id(mock_get, api_client):
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.json.return_value = {"error": "Team not found"}
    mock_get.return_value = mock_response

    with pytest.raises(Exception) as excinfo:
        api_client.get_team_players(team_id=9999)
    
    assert "API request failed with status code 404" in str(excinfo.value)
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/teams/9999/players",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_get_player_stats_no_stats(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"stats": {}}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    stats = api_client.get_player_stats(player_id=302)
    assert stats == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/players/302/stats",
        headers={"X-Auth-Token": "test_api_key"}
    )

@patch('app.api.api_client.requests.get')
def test_get_leagues_with_additional_params(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"leagues": [{"id": 2, "name": "La Liga"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    leagues = api_client.get_leagues(country="Spain", season=2023)
    assert leagues == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/leagues",
        headers={"X-Auth-Token": "test_api_key"},
        params={"country": "Spain", "season": 2023}
    )

@patch('app.api.api_client.requests.get')
def test_get_matches_no_league_id(mock_get, api_client):
    mock_response = Mock()
    expected_data = {"matches": [{"id": 103, "homeTeam": "Team E", "awayTeam": "Team F"}]}
    mock_response.json.return_value = expected_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    matches = api_client.get_matches()
    assert matches == expected_data
    mock_get.assert_called_once_with(
        f"{api_client.BASE_URL}/matches",
        headers={"X-Auth-Token": "test_api_key"},
        params={}
    )

@patch('app.api.api_client.requests.get')
def test_get_team_players_no_team_id(mock_get, api_client):
    with pytest.raises(TypeError):
        api_client.get_team_players()  # team_id is required, should raise TypeError
    mock_get.assert_not_called()

@patch('app.api.api_client.requests.get')
def test_get_player_stats_no_player_id(mock_get, api_client):
    with pytest.raises(TypeError):
        api_client.get_player_stats()  # player_id is required, should raise TypeError
    mock_get.assert_not_called()

# Note: The above tests assume that the ApiClient methods raise generic Exceptions on errors.
# In a real-world scenario, you might want to define and use custom exception classes for better
# error handling and testing.