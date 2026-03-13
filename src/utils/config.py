"""
config.py
---------
Module with utility functions to load configuration files.

Author: Ezau Faridh Torres Torres.
Date: March 2026.

Functions
---------
- load_params :
    Loads parameters from a JSON configuration file.
- load_contact_codes :
    Loads contact codes from a JSON configuration file.
"""
# Necessary imports
from json import load
from typing import Dict, Any

def load_params() -> Dict[str, Any]:
    """
    Load parameters from a JSON configuration file.
    """
    with open ("../config/params.json", "r") as f:
        return load(f)
    
def load_contact_codes() -> Dict:
    """
    Load contact codes from a JSON configuration file.
    """
    with open ("../config/contact_codes.json", "r") as f:
        return load(f)