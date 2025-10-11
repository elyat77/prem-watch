# update_db_oo.py
"""
An object-oriented script to update the SQLite database with data from the 
football data API.

This script can be run from the command line or imported and used
programmatically.
"""

import os
import argparse
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from app.api.api_client import ApiClient
from app.db.db_loader import SQLiteLoader

# --- Base Task Class (The Blueprint) ---

class Task(ABC):
    """Abstract base class for all update tasks."""
    is_general_task = True

    def __init__(self, client: ApiClient, loader: SQLiteLoader):
        self.client = client
        self.loader = loader

    @classmethod
    @abstractmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        """Class method to add task-specific arguments to the parser."""
        pass

    @abstractmethod
    def execute(self, **kwargs):
        """The main logic of the task."""
        pass


# --- Concrete Task Implementations ---

class UpdateLeaguesTask(Task):
    """Fetches and loads league and season data."""
    is_general_task = False

    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--country_id",
            type=int,
            help="[Leagues] Filter leagues by a specific country ID."
            )
        parser.add_argument(
            "--chosen_only",
            action="store_true",
            help="[Leagues] Fetch only leagues marked as chosen."
            )

    def execute(self, **kwargs) -> None:
        country_id = kwargs.get('country_id')
        chosen_only = kwargs.get('chosen_only')

        print("\n--- Updating Leagues ---")
        # Leagues data structure is a bit nested; we need to flatten it
        leagues_data = self.client.get_leagues(country_id=country_id, chosen_only=chosen_only)
        if not leagues_data or "data" not in leagues_data:
            print("Could not fetch leagues data or data is empty.")
            return
        leagues_data = leagues_data.get("data")
        cleaned_leagues = []
        for league in leagues_data:
                if "season" in league:
                    for season in league["season"]:
                        league_record = {
                            "id": season.get("id"),
                            "name": league.get("name"),
                            "season": season.get("year"),
                            "country": league.get("country"),
                            "league_name": league.get("league_name"),
                            "image": league.get("image")
                        }
                        cleaned_leagues.append(league_record)
        self.loader.ensure_table_and_columns('leagues', cleaned_leagues[0] if cleaned_leagues else {})
        for league in cleaned_leagues:
            self.loader.insert_or_update_dict('leagues', league)
        print("Leagues update complete.")


class UpdateCountriesTask(Task):
    """Fetches and loads country data."""
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        countries_data = self.client.get_countries()
        if not countries_data or "data" not in countries_data:
            print("Could not fetch countries data or data is empty.")
            return
        print("\n--- Updating Countries ---")
        countries_data = countries_data.get("data")
        self.loader.ensure_table_and_columns('countries', countries_data[0] if countries_data else {})
        for country in countries_data:
            self.loader.insert_or_update_dict('countries', country)
        print("Countries update complete.")


class UpdateMatchesTask(Task):
    """Fetches and loads match data for a given day."""
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        parser.add_argument("--date", type=str, help="[Matches] Specify a date in YYYY-MM-DD format.")
        
    def execute(self, **kwargs):
        date = kwargs.get('date')
        if date:
            print(f"\n--- Updating Matches for date: {date} ---")
            matches_data = self.client.get_matches(date=date)
        else:
            print("\n--- Updating Today's Matches ---")
            matches_data = self.client.get_matches()
        if not matches_data or "data" not in matches_data:
            print("Could not fetch matches data or data is empty.")
            return
        matches_data = matches_data.get("data")
        self.loader.ensure_table_and_columns('matches', matches_data[0] if matches_data else {})
        for match in matches_data:
            self.loader.insert_or_update_dict('matches', match)
        print("Matches update complete.")


class UpdateLeagueStatsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass # Shared args --season_id & --max_time already registered

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        max_time = kwargs.get('max_time')
        if not season_id:
            print("\nError: --season_id is required for the 'league_stats' task.")
            return
        print(f"\n--- Updating League Stats for season_id: {season_id} ---")
        league_stats_data = self.client.get_league_stats(season_id=season_id, max_time=max_time)
        if not league_stats_data or "data" not in league_stats_data:
            print("Could not fetch league stats data or data is empty.")
            return
        league_stats_data = league_stats_data.get("data")
        self.loader.ensure_table_and_columns('league_stats', league_stats_data[0] if league_stats_data else {})
        for league_stats in league_stats_data:
            self.loader.insert_or_update_dict('league_stats', league_stats)
        print("League Stats update complete.")


class UpdateSchedulesTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        max_time = kwargs.get('max_time')
        if not season_id:
            print("\nError: --season_id is required for the 'schedules' task.")
            return
        print(f"\n--- Updating Schedules for season_id: {season_id} ---")
        schedules_data = self.client.get_schedule(season_id=season_id, max_time=max_time)
        if not schedules_data or "data" not in schedules_data:
            print("Could not fetch schedules data or data is empty.")
            return
        schedules_data = schedules_data.get("data")
        self.loader.ensure_table_and_columns('matches', schedules_data[0] if schedules_data else {})
        for schedule in schedules_data:
            self.loader.insert_or_update_dict('matches', schedule)
        print("Schedules update complete.")


class UpdateTeamsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        parser.add_argument("--stats", action="store_true", help="[Teams] Include detailed stats when fetching teams.")

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        stats = kwargs.get('stats')
        max_time = kwargs.get('max_time')

        if not season_id:
            print("\nError: --season_id is required for the 'teams' task.")
            return

        teams_data = self.client.get_league_teams(season_id=season_id, stats=stats, max_time=max_time)
        if not teams_data or "data" not in teams_data:
            print("Could not fetch teams data or data is empty.")
            return
        teams_data = teams_data.get("data")
        print(f"\n--- Updating Teams for season_id: {season_id} ---")
        # If stats is selected, teams data contains nested dict which is a problem for flat table
        cleaned_teams = []
        for team in teams_data:
            team_record = team.copy()
            if stats and "stats" in team_record:
                stats_data = team_record.pop("stats")
                for key, value in stats_data.items():
                    team_record[f"stats_{key}"] = value
            cleaned_teams.append(team_record)

        self.loader.ensure_table_and_columns('teams', cleaned_teams[0] if cleaned_teams else {})
        for team in cleaned_teams:
            self.loader.insert_or_update_dict('teams', team)

        print("Teams update complete.")


class UpdatePlayersTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        max_time = kwargs.get('max_time')
        if not season_id:
            print("\nError: --season_id is required for the 'players' task.")
            return
        print(f"\n--- Updating Players for season_id: {season_id} ---")
        players_data = self.client.get_league_players(season_id=season_id, max_time=max_time)
        if not players_data or "data" not in players_data:
            print("Could not fetch players data or data is empty.")
            return
        players_data = players_data.get("data")
        self.loader.ensure_table_and_columns('players', players_data[0] if players_data else {})
        for player in players_data:
            self.loader.insert_or_update_dict('players', player)
        print("Players update complete.")


class UpdateRefereesTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'referees' task.")
            return
        print(f"\n--- Updating Referees for season_id: {season_id} ---")
        referees_data = self.client.get_league_referees(season_id=season_id)
        if not referees_data or "data" not in referees_data:
            print("Could not fetch referees data or data is empty.")
            return
        referees_data = referees_data.get("data")
        self.loader.ensure_table_and_columns('referees', referees_data[0] if referees_data else {})
        for referee in referees_data:
            self.loader.insert_or_update_dict('referees', referee)
        print("Referees update complete.")


class UpdateTeamDataTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        # team_id = kwargs.get('team_id')
        # if not team_id:
        #     print("\nError: --team_id is required for the 'team_data' task.")
        #     return
        # print(f"\n--- Updating Team Data for team_id: {team_id} ---")
        # print("Team Data update complete.")
        print("\n--- Use league teams task to update team data ---")
        return


class UpdateTeamFormTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        # team_id = kwargs.get('team_id')
        # if not team_id:
        #     print("\nError: --team_id is required for the 'team_form' task.")
        #     return
        # print(f"\n--- Updating Team Form for team_id: {team_id} ---")
        # print("Team Form update complete.")
        print("\n--- Use league teams task to update team form ---")
        return


class UpdateMatchDetailsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        match_id = kwargs.get('match_id')
        if not match_id:
            print("\nError: --match_id is required for the 'match_details' task.")
            return
        print(f"\n--- Updating Match Details for match_id: {match_id} ---")
        match_details_data = self.client.get_match_details(match_id=match_id)
        if not match_details_data or "data" not in match_details_data:
            print("Could not fetch match details data or data is empty.")
            return
        match_details_data = match_details_data.get("data")
        self.loader.ensure_table_and_columns('match_details', match_details_data[0] if match_details_data else {})
        for match_detail in match_details_data:
            self.loader.insert_or_update_dict('match_details', match_detail)
        print("Match Details update complete.")


class UpdateLeagueTableTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        max_time = kwargs.get('max_time')
        if not season_id:
            print("\nError: --season_id is required for the 'league_table' task.")
            return
        print(f"\n--- Updating League Table for season_id: {season_id} ---")
        league_table_data = self.client.get_league_table(season_id=season_id, max_time=max_time)
        if not league_table_data or "data" not in league_table_data:
            print("Could not fetch league table data or data is empty.")
            return
        league_table_data = league_table_data.get("data")
        self.loader.ensure_table_and_columns('league_table', league_table_data[0] if league_table_data else {})
        for league_table in league_table_data:
            self.loader.insert_or_update_dict('league_table', league_table)
        print("League Table update complete.")


class UpdatePlayerStatsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        player_id = kwargs.get('player_id')
        if not player_id:
            print("\nError: --player_id is required for the 'player_stats' task.")
            return
        print(f"\n--- Updating Player Stats for player_id: {player_id} ---")
        player_stats_data = self.client.get_player_stats(player_id=player_id)
        if not player_stats_data or "data" not in player_stats_data:
            print("Could not fetch player stats data or data is empty.")
            return
        player_stats_data = player_stats_data.get("data")
        self.loader.ensure_table_and_columns('players', player_stats_data[0] if player_stats_data else {})
        for player_stats in player_stats_data:
            self.loader.insert_or_update_dict('players', player_stats)
        print("Player Stats update complete.")


class UpdateRefereeStatsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        referee_id = kwargs.get('referee_id')
        if not referee_id:
            print("\nError: --referee_id is required for the 'referee_stats' task.")
            return
        print(f"\n--- Updating Referee Stats for referee_id: {referee_id} ---")
        referee_stats_data = self.client.get_referee_stats(referee_id=referee_id)
        if not referee_stats_data or "data" not in referee_stats_data:
            print("Could not fetch referee stats data or data is empty.")
            return
        referee_stats_data = referee_stats_data.get("data")
        self.loader.ensure_table_and_columns('referees', referee_stats_data[0] if referee_stats_data else {})
        for referee_stats in referee_stats_data:
            self.loader.insert_or_update_dict('referees', referee_stats)
        print("Referee Stats update complete.")


class UpdateBttsStatsTask(Task):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        print("\n--- Updating BTTS Stats ---")
        btts_data = self.client.get_btts_stats()
        if not btts_data or "data" not in btts_data:
            print("Could not fetch BTTS stats data or data is empty.")
            return
        btts_data = btts_data.get("data")
        # btts_data is dict not list (for some reason) so next line fails TODO: fix
        # self.loader.ensure_table_and_columns('btts_stats', btts_data[0] if btts_data else {})
        for btts_stats in btts_data:
            self.loader.insert_or_update_dict('btts_stats', btts_stats)
        print("BTTS Stats update complete.")


class UpdateOver25StatsTask(Task):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        print("\n--- Updating Over 2.5 Goals Stats ---")
        over_25_data = self.client.get_over_25_stats()
        if not over_25_data or "data" not in over_25_data:
            print("Could not fetch Over 2.5 stats data or data is empty.")
            return
        over_25_data = over_25_data.get("data")
        # over_25_data is dict not list (for some reason) so next line fails TODO: fix
        # self.loader.ensure_table_and_columns('over_25_stats', over_25_data[0] if over_25_data else {})
        for over_25_stats in over_25_data:
            self.loader.insert_or_update_dict('over_25_stats', over_25_stats)
        print("Over 2.5 Stats update complete.")


class ComprehensiveUpdateTask(Task):
    """A special task that orchestrates a full, cascading database update."""
    is_general_task = False

    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        # This task orchestrates others and adds no arguments itself.
        pass

    def _get_ids_from_table(self, table_name: str, column_name: str) -> list:
        """Helper to query the database and fetch a list of unique IDs."""
        ids = []
        try:
            cursor = self.loader.conn.cursor()
            # Check if table exists before querying
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if cursor.fetchone() is None:
                print(f"Warning: Table '{table_name}' not found for querying. Skipping dependent tasks.")
                return []
                
            cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL")
            results = cursor.fetchall()
            ids = [row[0] for row in results]
        except Exception as e:
            print(f"Error querying {column_name} from {table_name}: {e}")
        return ids

    def execute(self, **kwargs):
        print("\n--- Starting Comprehensive Database Update ---")
        
        # We need access to all other task classes to instantiate them
        registered_tasks = kwargs.get('registered_tasks', {})
        task_instances = {
            name: task_class(self.client, self.loader)
            for name, task_class in registered_tasks.items() if name != 'comprehensive_update'
        }

        def run_task(task_name, **params):
            if task_name in task_instances:
                task_instances[task_name].execute(**params)
            else:
                print(f"Warning: Orchestrator tried to run unregistered task '{task_name}'.")

        # LEVEL 0: Run tasks with no dependencies
        print("\n[LEVEL 0] Running initial tasks...")
        run_task('countries')
        # No need to run matches here, they will be fetched as part of schedules TODO: remove
        # run_task('matches') # Gets today's matches by default
        # Data returned by following tasks is different TODO: fix
        # run_task('btts_stats')
        # run_task('over_25_stats')

        # LEVEL 1: Depends on Countries
        print("\n[LEVEL 1] Updating leagues based on countries...")
        country_ids = self._get_ids_from_table('countries', 'id')
        for country_id in country_ids:
            run_task('leagues', country_id=country_id, chosen_only=False)

        # LEVEL 2: Depends on Leagues (which contain season_id)
        print("\n[LEVEL 2] Updating season-dependent data...")
        season_ids = self._get_ids_from_table('leagues', 'id')
        season_tasks = ['league_stats', 'schedules', 'teams', 'players', 'referees', 'league_table']
        for season_id in season_ids:
            for task_name in season_tasks:
                run_task(task_name, season_id=season_id, stats=True) # Stats=True should work, other funcs should ignore it

        # LEVEL 3: Depends on Teams and Matches
        print("\n[LEVEL 3] Updating team and match details...")
        team_ids = self._get_ids_from_table('teams', 'id')
        for team_id in team_ids:
            run_task('team_data', team_id=team_id)
            run_task('team_form', team_id=team_id)
        
        match_ids = self._get_ids_from_table('matches', 'id')
        for match_id in match_ids:
            run_task('match_details', match_id=match_id)

        # LEVEL 4: Depends on Players and Referees
        print("\n[LEVEL 4] Updating player and referee stats...")
        player_ids = self._get_ids_from_table('players', 'id')
        for player_id in player_ids:
            run_task('player_stats', player_id=player_id)
            
        referee_ids = self._get_ids_from_table('referees', 'id')
        for referee_id in referee_ids:
            run_task('referee_stats', referee_id=referee_id)

        print("\n--- Comprehensive Update Complete ---")

# --- The Main Orchestrator ---

class DatabaseUpdater:
    """Manages the registration and execution of all tasks."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.client = None
        self.loader = None
        self.registered_tasks = {
            # Special orchestrator task
            'comprehensive_update': ComprehensiveUpdateTask,
            # Individual tasks
            'leagues': UpdateLeaguesTask, 'countries': UpdateCountriesTask, 'matches': UpdateMatchesTask,
            'league_stats': UpdateLeagueStatsTask, 'schedules': UpdateSchedulesTask, 'teams': UpdateTeamsTask,
            'players': UpdatePlayersTask, 'referees': UpdateRefereesTask, 'team_data': UpdateTeamDataTask,
            'team_form': UpdateTeamFormTask, 'match_details': UpdateMatchDetailsTask, 'league_table': UpdateLeagueTableTask,
            'player_stats': UpdatePlayerStatsTask, 'referee_stats': UpdateRefereeStatsTask, 'btts_stats': UpdateBttsStatsTask,
            'over_25_stats': UpdateOver25StatsTask,
        }
        self._setup()

    def _setup(self):
        load_dotenv(); api_key = os.getenv("API_KEY")
        if not api_key: raise ValueError("API_KEY not found in .env file.")
        db_file_path = os.path.abspath(self.db_path)
        print(f"Using database file: {db_file_path}")
        self.client = ApiClient(api_key)
        self.loader = SQLiteLoader(db_file_path)

    def run_tasks(self, task_names: list, **kwargs):
        print(f"\nExecuting tasks: {', '.join(task_names)}")
        try:
            for task_name in task_names:
                if task_name not in self.registered_tasks:
                    print(f"Warning: Task '{task_name}' is not registered. Skipping."); continue
                task_class = self.registered_tasks[task_name]
                task_instance = task_class(self.client, self.loader)
                # Pass the full registry to the comprehensive task so it can create other tasks
                if task_name == 'comprehensive_update':
                    kwargs['registered_tasks'] = self.registered_tasks
                task_instance.execute(**kwargs)
        finally:
            if self.loader: self.loader.close(); print("\nDatabase operations complete.")
    
    def run_from_cli(self):
        parser = self._create_parser()
        args = parser.parse_args()

        if args.all:
            # --all now exclusively runs the comprehensive update orchestrator
            tasks_to_run_names = ['comprehensive_update']
        elif args.task:
            tasks_to_run_names = args.task
        else:
            print("No tasks specified. Use --all or --task. Use -h for help."); return

        cli_kwargs = vars(args)
        self.run_tasks(tasks_to_run_names, **cli_kwargs)

    def _create_parser(self):
        parser = argparse.ArgumentParser(description="Update the football data SQLite database from an API.", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("db_path", type=str, help="Path to the SQLite database file.")
        parser.add_argument("--task", type=str, nargs='+', choices=list(self.registered_tasks.keys()), metavar='TASK', help="Specify one or more tasks to run.")
        parser.add_argument("--all", action="store_true", help="Run a comprehensive, cascading update of the entire database.")

        # Shared arguments
        parser.add_argument("--season_id", type=int, help="ID for a specific season.")
        parser.add_argument("--max_time", type=int, help="Unix timestamp for ending point of data.")
        parser.add_argument("--team_id", type=int, help="ID for a specific team.")
        parser.add_argument("--match_id", type=int, help="ID for a specific match.")
        parser.add_argument("--player_id", type=int, help="ID for a specific player.")
        parser.add_argument("--referee_id", type=int, help="ID for a specific referee.")

        # Task-specific arguments
        for task_class in self.registered_tasks.values():
            task_class.register_arguments(parser)
        return parser

# --- Script Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("db_path", nargs='?', default="footystats.db", help="Path to the SQLite database file.")
    known_args, _ = parser.parse_known_args()
    updater = DatabaseUpdater(db_path=known_args.db_path)
    updater.run_from_cli()
