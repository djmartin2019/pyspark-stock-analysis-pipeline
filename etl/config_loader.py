import json
import os
from pathlib import Path
from dotenv import load_dotenv

def load_config():
    # Load environment variables from .env file
    load_dotenv(Path(__file__).parent.parent / ".env")
    
    # Load base configuration from JSON
    with open(Path(__file__).parent.parent / "config/config.json") as f:
        config = json.load(f)
    
    # Add environment variables to config
    config["alpha_vantage_key"] = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    # Validate that API key is present
    if not config["alpha_vantage_key"]:
        raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables. Please check your .env file.")
    
    return config
