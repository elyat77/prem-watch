import soccerdata as sd #Not compatible with Py3.13 yet
import pandas as pd
from premwatch.utils.scraping import TorManager
from dotenv import load_dotenv
import signal
import os


def signal_handler(sig, frame):
    """Handles Ctrl+C exit"""
    print(f"Caught signal {sig}. Starting shutdown...")
    exit(0)

if __name__ == "__main__":

    # Register signals for nice shutdowns (closing Tor instance)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load environment variables
    load_dotenv()

    # Load paths
    CACHE_ROOT = "./data_cache/" # Put this in env too, but relative path is fine for now
    tor_path = os.getenv("TOR_PATH")

    # Make sure Tor is running to hide IP
    tor_manager = TorManager(tor_path=tor_path)
    if not tor_manager.is_tor_running():
        tor_manager.start()

    # Build scraping classes
    cache_path = CACHE_ROOT + "ClubElo/"
    club_elo = sd.ClubElo(proxy="tor", data_dir=cache_path)

    today_results = club_elo.read_by_date()
    print(today_results)
