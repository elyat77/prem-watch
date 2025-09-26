# update_db.py
"""Script to update the SQLite database with data from the football data API.

This script creates or updates the database in the same folder as the file.
"""

import sqlite3
import json
import os
import re
from dotenv import load_dotenv
from api.api_client import ApiClient
from db.db_loader import SQLiteLoader


if __name__ == "__main__":

    load_dotenv()
    api_key = os.getenv("API_KEY")
    client = ApiClient(api_key)

    # Request path for db (existing or new)
    db_path = ""
    while not re.match(r'^[\w\s-]+\.db$', db_path):
        db_path = input("Enter the path for the SQLite database (e.g., footystats.db): ").strip()
        print("Error: Invalid database filename. Please ensure filename contains valid characters and ends with .db.")
    else:
        print(f"Using database file: {db_path}")
    
    # This is to write the db file to the same directory as the script
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db_path = os.path.join(dir_path, db_path)

    # Initialize API client and database loader
    api_client = ApiClient(api_key)
    db_loader = SQLiteLoader(db_path)

    # Leagues
    # Data is a weird shape, shift so that each unique season is a row
    leagues_data = api_client.get_leagues()
    new_leagues = []
    for league in leagues_data["data"]:
        for season in league["season"]:
            league_record = {
                "id": season["id"],
                "name": league["name"],
                "season": season["year"],
                "country": league["country"],
                "league_name": league["name"],
                "image": league["image"],
            }
            new_leagues.append(league_record)
            print(f"Prepared record for league: {league.name}, season: {season}")
    
    for league_record in new_leagues:
        print(f"Loading season: {league_record['name']} {league_record['season']}")
        db_loader.insert_or_update_dict("leagues", league_record)

    # Countries
    countries_data = api_client.get_countries()
    if isinstance(countries_data["data"], list):
        for country in countries_data["data"]:
            print(f"Loading country: {country.country}")
            db_loader.insert_or_update_dict("countries", country)


    # Close the connection
    db_loader.close()
    print(f"\nDatabase operations complete. Check {db_path}.")
