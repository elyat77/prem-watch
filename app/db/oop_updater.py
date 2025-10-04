# update_db_oo.py
"""
An object-oriented script to update the SQLite database with data from the 
football data API.

This script can be run from the command line or imported and used programmatically.
"""

import os
import argparse
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from api.api_client import ApiClient
from db.db_loader import SQLiteLoader

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
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        parser.add_argument("--country_id", type=int, help="[Leagues] Filter leagues by a specific country ID.")
        parser.add_argument("--chosen_only", action="store_true", help="[Leagues] Fetch only leagues marked as chosen.")

    def execute(self, **kwargs):
        print("\n--- Updating Leagues ---")
        # Placeholder for your actual API call and data handling
        country_id = kwargs.get('country_id')
        chosen_only = kwargs.get('chosen_only')
        print(f"Params: country_id={country_id}, chosen_only={chosen_only}")
        print("Leagues update complete.")

class UpdateCountriesTask(Task):
    """Fetches and loads country data."""
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        print("\n--- Updating Countries ---")
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
        else:
            print("\n--- Updating Today's Matches ---")
        print("Matches update complete.")

class UpdateLeagueStatsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass # Args registered by other tasks

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'league_stats' task.")
            return
        print(f"\n--- Updating League Stats for season_id: {season_id} ---")
        print("League Stats update complete.")

class UpdateSchedulesTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'schedules' task.")
            return
        print(f"\n--- Updating Schedules for season_id: {season_id} ---")
        print("Schedules update complete.")

class UpdateTeamsTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        parser.add_argument("--stats", action="store_true", help="[Teams] Include detailed stats when fetching teams.")

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'teams' task.")
            return
        print(f"\n--- Updating Teams for season_id: {season_id} ---")
        print("Teams update complete.")

class UpdatePlayersTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'players' task.")
            return
        print(f"\n--- Updating Players for season_id: {season_id} ---")
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
        print("Referees update complete.")

class UpdateTeamDataTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        team_id = kwargs.get('team_id')
        if not team_id:
            print("\nError: --team_id is required for the 'team_data' task.")
            return
        print(f"\n--- Updating Team Data for team_id: {team_id} ---")
        print("Team Data update complete.")

class UpdateTeamFormTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        team_id = kwargs.get('team_id')
        if not team_id:
            print("\nError: --team_id is required for the 'team_form' task.")
            return
        print(f"\n--- Updating Team Form for team_id: {team_id} ---")
        print("Team Form update complete.")

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
        print("Match Details update complete.")

class UpdateLeagueTableTask(Task):
    is_general_task = False
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        season_id = kwargs.get('season_id')
        if not season_id:
            print("\nError: --season_id is required for the 'league_table' task.")
            return
        print(f"\n--- Updating League Table for season_id: {season_id} ---")
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
        print("Referee Stats update complete.")

class UpdateBttsStatsTask(Task):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        print("\n--- Updating BTTS Stats ---")
        print("BTTS Stats update complete.")

class UpdateOver25StatsTask(Task):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser):
        pass

    def execute(self, **kwargs):
        print("\n--- Updating Over 2.5 Goals Stats ---")
        print("Over 2.5 Stats update complete.")

# --- The Main Orchestrator ---

class DatabaseUpdater:
    """Manages the registration and execution of all tasks."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.client = None
        self.loader = None
        self.registered_tasks = {
            'leagues': UpdateLeaguesTask,
            'countries': UpdateCountriesTask,
            'matches': UpdateMatchesTask,
            'league_stats': UpdateLeagueStatsTask,
            'schedules': UpdateSchedulesTask,
            'teams': UpdateTeamsTask,
            'players': UpdatePlayersTask,
            'referees': UpdateRefereesTask,
            'team_data': UpdateTeamDataTask,
            'team_form': UpdateTeamFormTask,
            'match_details': UpdateMatchDetailsTask,
            'league_table': UpdateLeagueTableTask,
            'player_stats': UpdatePlayerStatsTask,
            'referee_stats': UpdateRefereeStatsTask,
            'btts_stats': UpdateBttsStatsTask,
            'over_25_stats': UpdateOver25StatsTask,
        }
        self._setup()

    def _setup(self):
        """Initializes the API client and database loader."""
        load_dotenv()
        api_key = os.getenv("API_KEY")
        if not api_key:
            raise ValueError("API_KEY not found in .env file.")
        
        db_file_path = os.path.abspath(self.db_path)
        print(f"Using database file: {db_file_path}")
        self.client = ApiClient(api_key)
        self.loader = SQLiteLoader(db_file_path)

    def run_tasks(self, task_names: list, **kwargs):
        """Executes a list of specified tasks with given keyword arguments."""
        print(f"\nExecuting tasks: {', '.join(task_names)}")
        try:
            for task_name in task_names:
                if task_name not in self.registered_tasks:
                    print(f"Warning: Task '{task_name}' is not registered. Skipping.")
                    continue
                
                task_class = self.registered_tasks[task_name]
                task_instance = task_class(self.client, self.loader)
                task_instance.execute(**kwargs)
        finally:
            if self.loader:
                self.loader.close()
            print("\nDatabase operations complete.")
    
    def run_from_cli(self):
        """Parses command-line arguments and runs the specified tasks."""
        parser = self._create_parser()
        args = parser.parse_args()

        tasks_to_run_names = []
        if args.all:
            tasks_to_run_names = [name for name, cls in self.registered_tasks.items() if cls.is_general_task]
        elif args.task:
            tasks_to_run_names = args.task
        else:
            print("No tasks specified. Use --all or --task. Use -h for help.")
            return

        cli_kwargs = vars(args)
        self.run_tasks(tasks_to_run_names, **cli_kwargs)

    def _create_parser(self):
        """Creates and configures the argument parser."""
        parser = argparse.ArgumentParser(
            description="Update the football data SQLite database from an API.",
            formatter_class=argparse.RawTextHelpFormatter
        )
        parser.add_argument("db_path", type=str, help="Path to the SQLite database file.")
        parser.add_argument(
            "--task", type=str, nargs='+', choices=list(self.registered_tasks.keys()),
            metavar='TASK', help="Specify one or more tasks to run."
        )
        general_tasks = [name for name, cls in self.registered_tasks.items() if cls.is_general_task]
        parser.add_argument("--all", action="store_true", help=f"Run all general tasks: {', '.join(general_tasks)}")

        # --- Register Shared Arguments to avoid conflicts and repetition ---
        parser.add_argument("--season_id", type=int, help="ID for a specific season.")
        parser.add_argument("--max_time", type=int, help="Unix timestamp for ending point of data.")
        parser.add_argument("--team_id", type=int, help="ID for a specific team.")
        parser.add_argument("--match_id", type=int, help="ID for a specific match.")
        parser.add_argument("--player_id", type=int, help="ID for a specific player.")
        parser.add_argument("--referee_id", type=int, help="ID for a specific referee.")

        # Let each task register its own unique arguments
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
