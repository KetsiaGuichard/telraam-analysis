import os
import pandas as pd
from yaml import safe_load
from src.external_data_fetcher import get_public_holidays, get_vacations


def get_telraam_information():
    """Give all informations about segments and sensors in config.

    Returns:
        pd.DataFrame: Sensors information and their segment's name

    Examples:
        >>> isinstance(get_telraam_information(), pd.DataFrame)
        True
    """
    data_dict = {}

    for file_type in ["sensors", "segments"]:
        try:
            f = open(f"config/{file_type}.yaml", "r")
        except FileNotFoundError:
            print(f"{file_type} file doesn't exist")
            return pd.DataFrame()
        data_dict[file_type] = pd.DataFrame(safe_load(f)).transpose()

    sensors = data_dict["sensors"].merge(
        data_dict["segments"], on="segment_id", how="left"
    )
    sensors["segment_fullname"] = (
        sensors["segment_id"].astype(str) + " - " + sensors["segment_name"]
    )

    return sensors


class EnrichedDataLoader:
    """Retrieve locally stored data and enhance it with additional information.

    Args:
        directory (str): Directory where the data are stored. Default to "data".
    """

    def __init__(
        self,
        directory: str = "data",
    ):
        """Initializes an instance of the APIFetcher class."""
        self.directory = directory
        self.raw_data = self.__retrieve_telraam_data()
        self.__enriched_data = self.raw_data

    def __retrieve_telraam_data(self):
        """Retrieve stored data in specified directory.
        If more than one file available, concatenate all files and remove duplicates.

        Returns:
            pd.DataFrame: A dataframe with all stored data in the directory.
        """
        files = [
            f
            for f in os.listdir(self.directory)
            if os.path.isfile(f"{self.directory}/{f}")
        ]
        if len(files) == 0:
            return pd.DataFrame
        raw_data = pd.DataFrame()
        for file in files:
            tmp_data = pd.read_csv(f"data/{file}")
            raw_data = pd.concat([raw_data, tmp_data], ignore_index=True)
        raw_data = raw_data.drop_duplicates()
        return raw_data

    def get_raw_data(self):
        """Get Raw data."""
        return self.raw_data

    def __enrich_dates(self):
        """Add calendar informations in a dataframe."""
        data_dates = self.__enriched_data.copy()
        data_dates["date"] = pd.to_datetime(data_dates["date"]).dt.tz_convert(
            "Europe/Paris"
        )
        data_dates["day"] = data_dates["date"].dt.date
        data_dates["day_of_month"] = data_dates["date"].dt.day
        data_dates["hour"] = data_dates["date"].dt.hour
        data_dates["weekday"] = [day.strftime("%A") for day in data_dates["day"]]
        data_dates["week_number"] = data_dates["date"].dt.isocalendar().week
        data_dates["month"] = data_dates["date"].dt.month
        data_dates["year"] = data_dates["date"].dt.year
        self.__enriched_data = data_dates

    def __enrich_sensors(self):
        """Add sensors informations."""
        data_sensors = self.__enriched_data.copy()
        sensors = get_telraam_information()
        data_sensors = data_sensors.merge(
            sensors[["instance_id", "segment_id", "segment_fullname"]],
            on="instance_id",
            how="left",
        )
        self.__enriched_data = data_sensors

    def __enrich_special_events(self):
        """Add public holidays and vacations"""
        data_sensors = self.__enriched_data.copy()

        # public holidays
        public_holidays = get_public_holidays()
        data_sensors = data_sensors.merge(public_holidays, on="day", how="left")
        data_sensors = data_sensors.fillna(
            {"public_holiday_flag": 0, "public_holiday": "No public holiday"}
        )

        # vacations
        vacations = get_vacations().sort_values(by="start_date")
        data_sensors = data_sensors.sort_values(by="date")
        data_sensors = pd.merge_asof(
            data_sensors,
            vacations,
            left_on="date",
            right_on="start_date",
            direction="backward",
        )
        mask = (data_sensors["date"] >= data_sensors["start_date"]) & (
            data_sensors["date"] <= data_sensors["end_date"]
        )
        data_sensors["vacation_flag"] = (
            mask & data_sensors["vacation_flag"].notnull()
        ).astype(int)
        data_sensors.loc[data_sensors["vacation_flag"] == 0, "vacation"] = "No vacation"

        self.__enriched_data = data_sensors

    def get_enriched_data(self):
        """Get enriched data."""
        self.__enrich_dates()
        self.__enrich_sensors()
        self.__enrich_special_events()
        return self.__enriched_data
