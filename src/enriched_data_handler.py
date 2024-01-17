import os
import pandas as pd
from yaml import safe_load


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
        data_dates["date"] = pd.to_datetime(data_dates["date"])
        data_dates["day"] = data_dates["date"].dt.date
        data_dates["hour"] = data_dates["date"].dt.hour
        data_dates["weekday"] = [day.strftime("%A") for day in data_dates["day"]]
        data_dates["week_number"] = data_dates["date"].dt.isocalendar().week
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

    def get_enriched_data(self):
        """Get enriched data."""
        self.__enrich_dates()
        self.__enrich_sensors()
        return self.__enriched_data
