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


def update_fixtures(client: ApiClient, loader: SQLiteLoader):
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
    parser.add_argument("db_path", type=str, help="Path to the SQLite database file (e.g., footystats.db)")
    parser.add_argument("--all", action="store_true", help="Run all update tasks.")
    parser.add_argument("--leagues", action="store_true", help="Update the leagues table.")
    parser.add_argument("--countries", action="store_true", help="Update the countries table.")
    parser.add_argument("--fixtures", action="store_true", help="Update today's fixtures.")
    # Add new arguments here when additional functions are implemented
    
    args = parser.parse_args()

    # Load environment variables (for API_KEY)
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API_KEY not found in .env file.")
        return

    # Construct absolute path for the database file
    db_file_path = os.path.abspath(args.db_path)
    print(f"Using database file: {db_file_path}")

    # Initialize API client and database loader
    api_client = ApiClient(api_key)
    db_loader = SQLiteLoader(db_file_path)

    try:
        run_all = args.all or not any([args.leagues, args.countries, args.fixtures])

        if run_all or args.leagues:
            update_leagues(api_client, db_loader)
        
        if run_all or args.countries:
            update_countries(api_client, db_loader)
            
        if run_all or args.fixtures:
            update_fixtures(api_client, db_loader)
            
        # Add other update function calls here...

    finally:
        # Ensure the database connection is always closed
        db_loader.close()
        print(f"\nDatabase operations complete.")


if __name__ == "__main__":
    main()
