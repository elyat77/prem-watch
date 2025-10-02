# update_db.py
"""Script to update the SQLite database with data from the football data API.

This script creates or updates the database in the same folder as the file (default /app/db/).

Command line arguments can be used when running the script to specify which data to update.

Example usage:
    - Run all updates: python update_db.py footystats.db --all
    - Update only leagues and countries: python update_db.py footystats.db --leagues --countries
"""

import sqlite3
import json
import os
import argparse
import re
from dotenv import load_dotenv
from api.api_client import ApiClient
from db_loader import SQLiteLoader


def update_leagues(client: ApiClient, loader: SQLiteLoader):
    """Fetches leagues data, transforms it, and loads it to the database."""
    print("\n--- Updating Leagues ---")
    try:
        leagues_data = client.get_leagues()
        if not leagues_data or "data" not in leagues_data:
            print("Could not fetch leagues data or data is empty.")
            return

        new_leagues = []
        if isinstance(leagues_data["data"], list):
            # The API returns seasons nested under each league, so we flatten it.
            for league in leagues_data["data"]:
                if "season" in league:
                    for season in league["season"]:
                        league_record = {
                            "id": season.get("id"),
                            "name": league.get("name"),
                            "season": season.get("year"),
                            "country": league.get("country"),
                            "league_name": league.get("name"),
                            "image": league.get("image")
                        }
                        new_leagues.append(league_record)
        
        for league_record in new_leagues:
            print(f"Loading record for: {league_record.get('name')} {league_record.get('season')}")
            loader.insert_or_update_dict("leagues", league_record)
        print("Leagues update complete.")
    except Exception as e:
        print(f"An error occurred during leagues update: {e}")


def update_countries(client: ApiClient, loader: SQLiteLoader):
    """Fetches countries data and loads it to the database."""
    print("\n--- Updating Countries ---")
    try:
        countries_data = client.get_countries()
        if not countries_data or "data" not in countries_data:
            print("Could not fetch countries data or data is empty.")
            return

        if isinstance(countries_data["data"], list):
            for country in countries_data["data"]:
                print(f"Loading country: {country.get('country')}")
                loader.insert_or_update_dict("countries", country)
        print("Countries update complete.")
    except Exception as e:
        print(f"An error occurred during countries update: {e}")


def todays_fixtures(client: ApiClient, loader: SQLiteLoader):
    """Fetches today's fixtures and loads them to the database."""
    print("\n--- Updating Today's Fixtures ---")
    try:
        fixtures_data = client.get_fixtures()
        if not fixtures_data or "data" not in fixtures_data:
            print("Could not fetch fixtures data or data is empty.")
            return

        if isinstance(fixtures_data["data"], list):
            for fixture in fixtures_data["data"]:
                print(f"Loading fixture ID: {fixture.get('id')} - {fixture.get('homeTeam', {}).get('name')} vs {fixture.get('awayTeam', {}).get('name')}")
                loader.insert_or_update_dict("fixtures", fixture)
        print("Fixtures update complete.")
    except Exception as e:
        print(f"An error occurred during fixtures update: {e}")


def main():
    """Main function to parse arguments and run updates."""
    parser = argparse.ArgumentParser(description="Update the football data SQLite database from an API.")
    # --- Positional Argument ---
    parser.add_argument("db_path", type=str, help="Path to the SQLite database file (e.g., footystats.db)")
    
    # --- Task Arguments (Flags to enable a task) ---
    parser.add_argument("--all", action="store_true", help="Run all general update tasks (those not requiring specific IDs).")
    parser.add_argument("--leagues", action="store_true", help="Update the leagues table. Optional: --country_id, --chosen_only")
    parser.add_argument("--countries", action="store_true", help="Update the countries table.")
    parser.add_argument("--matches", action="store_true", help="Update the matches table with games from a given day (gets today's by default). Optional: --date.")
    parser.add_argument("--league_stats", action="store_true", help="Update the league_stats table. Requires: --season_id. Optional: --max_time.")
    parser.add_argument("--schedules", action="store_true", help="Update the schedules table. Requires: --season_id. Optional: --max_time.")
    parser.add_argument("--teams", action="store_true", help="Update the teams table. Requires: --season_id. Optional: --stats, --max_time.")
    parser.add_argument("--players", action="store_true", help="Update the players table. Requires: --season_id. Optional: --max_time.")
    parser.add_argument("--referees", action="store_true", help="Update the referees table. Requires: --season_id. Optional: --max_time.")
    parser.add_argument("--team_data", action="store_true", help="Update detailed data for a specific team. Requires: --team_id.")
    parser.add_argument("--team_form", action="store_true", help="Update recent form data for a specific team. Requires: --team_id.")
    parser.add_argument("--match_details", action="store_true", help="Update detailed data for a specific match. Requires: --match_id.")
    parser.add_argument("--league_table", action="store_true", help="Update the league_table table. Requires: --season_id. Optional: --max_time.")
    parser.add_argument("--player_stats", action="store_true", help="Update player statistics. Requires: --player_id.")
    parser.add_argument("--referee_stats", action="store_true", help="Update referee statistics. Requires: --referee_id.")
    parser.add_argument("--btts_stats", action="store_true", help="Update both teams to score statistics.")
    parser.add_argument("--over_25_stats", action="store_true", help="Update over 2.5 goals statistics.")

    # --- Parameter Arguments (Values for the tasks) ---
    parser.add_argument("--date", type=str, help="Specify a date in YYYY-MM-DD format.")
    parser.add_argument("--country_id", type=int, help="Specify the country_id for fetching leagues.")
    parser.add_argument("--season_id", type=int, help="Specify the season for fetching league-specific data.")
    parser.add_argument("--team_id", type=int, help="Specify the id of the team to fetch data.")
    parser.add_argument("--match_id", type=int, help="Specify the id of the match to fetch data for.")
    parser.add_argument("--player_id", type=int, help="Specify the player_id for fetching player stats.")
    parser.add_argument("--referee_id", type=int, help="Specify the referee_id for fetching referee stats.")
    parser.add_argument("--chosen_only", type=bool, help="When fetching leagues, if True, only fetch leagues marked as chosen.")
    parser.add_argument("--max_time", type=int, help="Specify the ending point for fetched data in unix format.")
    parser.add_argument("--stats", type=bool, help="When fetching teams, if True, include detailed stats.")

    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API_KEY not found in .env file.")
        return

    db_file_path = os.path.abspath(args.db_path)
    print(f"Using database file: {db_file_path}")

    api_client = ApiClient(api_key)
    db_loader = SQLiteLoader(db_file_path)

    try:
        # Determine if any specific task was requested. If not, --all is implied.
        tasks_requested = any([args.leagues, args.countries, args.fixtures, args.players])
        run_all = args.all or not tasks_requested

        # --- Execute Tasks ---
        if run_all or args.leagues:
            update_leagues(api_client, db_loader)
        
        if run_all or args.countries:
            update_countries(api_client, db_loader)
            
        if run_all or args.fixtures:
            # Pass the date argument. It will be None if the user didn't provide it.
            update_fixtures(api_client, db_loader, date=args.date)

        if args.players:
            # This task requires a specific parameter, so it's not run with --all.
            if args.team_id:
                update_players(api_client, db_loader, team_id=args.team_id)
            else:
                print("\nError: --team_id must be provided when using the --players flag.")
            
    finally:
        db_loader.close()
        print(f"\nDatabase operations complete.")


if __name__ == "__main__":
    main()
