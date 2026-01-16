import os
import json
import requests
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
        "select": "description AS vacation, start_date, end_date, 1 AS vacation_flag",
        "refine": "location:Rennes",
        "exclude": "population:Enseignants",
    }
    response = requests.get(os.getenv("VACATIONS_URL"), params=payload, timeout=30)
    vacations = json2pandas(response.text, "")
    vacations["start_date"] = pd.to_datetime(
        vacations["start_date"], errors="coerce"
    ).dt.tz_convert("Europe/Paris")
    vacations["end_date"] = pd.to_datetime(
        vacations["end_date"], errors="coerce"
    ).dt.tz_convert("Europe/Paris")
    return vacations


def get_public_holidays():
    """Public Holidays, with the default being the french dates from a governmental API.

    Returns:
        pd.DataFrame: Description, date of public holidays.
    """
    response = requests.get(os.getenv("HOLIDAYS_URL"), timeout=30)
    holidays_dict = json.loads(response.text)
    holidays = pd.DataFrame(
        {
            "day": holidays_dict.keys(),
            "public_holiday": holidays_dict.values(),
            "public_holiday_flag": 1,
        }
    )
    holidays["day"] = pd.to_datetime(holidays["day"]).dt.date
    return holidays
