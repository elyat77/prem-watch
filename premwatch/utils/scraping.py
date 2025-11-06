import socket
import os
import subprocess
import atexit

class TorManager:
    """
    A class to manage the lifecycle of a Tor process. If no Tor process is running, 
    one can be started and will be automatically terminated when the program exits.
    Uses standalone Tor app (downloaded with 'expert package') which uses port 9050
    by default. Will not work with just Tor browser.
    
    Attributes:
        tor_path (str): The file path to the Tor executable.
    """
    
    def __init__(self, tor_path: str) -> None:
        self.tor_path = tor_path
        self.process = None

        atexit.register(self.cleanup)

    def is_tor_running(self) -> bool:
        """Checks if port 9050 is open (standalone Tor)"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect(("127.0.0.1", 9050))
            return True
        except (socket.timeout, socket.error):
            return False
        finally:
            s.close()

    def start(self) -> bool:
        """Starts the Tor process if it's not already running."""
        if not self.tor_path or not os.path.isfile(self.tor_path):
            print(f"Tor executable not found at {self.path}.")
            return False
        if self.is_tor_running():
            print("Tor (or another service) is already running on port 9050.")
            return True
        
        try:
            print(f"Launching Tor from {self.tor_path}...")
            self.process = subprocess.Popen([self.tor_path])
            print(f"Tor process started with PID: {self.process.pid}")
            return True
        except Exception as e:
            print(f"Failed to launch Tor: {e}")
            return False
        
    def cleanup(self) -> None:
        """Politely shuts down the Tor process if it was started by this class."""
        if self.process and self.process.poll() is None:
            print(f"Shutting down Tor process (PID: {self.process.pid})...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
                print("Tor process shut down successfully.")
            except subprocess.TimeoutExpired:
                print("Tor process didn't fancy shutting down, killing it...")
                self.process.kill()
        else:
            print("I didn't start Tor, I won't close it.")
