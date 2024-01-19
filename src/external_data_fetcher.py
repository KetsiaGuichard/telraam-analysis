import requests
import os
import json
import pandas as pd
from dotenv import load_dotenv
from src.api_fetcher import json2pandas

# Get environment variables
load_dotenv()


def get_vacations():
    """Public vacations, with the default being the french dates from a governmental API.

    Returns:
        pd.DataFrame: Description, start date and end date of vacations.
    """
    payload = {
        "select": "description, start_date, end_date",
        "refine": "location:Rennes",
        "exclude": "population:Enseignants"
    }
    response = requests.get(os.getenv("VACATIONS_URL"), params=payload)
    return json2pandas(response.text, '')

def get_public_holidays():
    """Public Holidays, with the default being the french dates from a governmental API.

    Returns:
        pd.DataFrame: Description, date of public holidays.
    """
    response = requests.get(os.getenv("HOLIDAYS_URL"))
    holidays_dict = json.loads(response.text)
    holidays = pd.DataFrame({'date':holidays_dict.keys(), 'description':holidays_dict.values()})
    return holidays

get_public_holidays()
