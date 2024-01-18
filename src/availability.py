import numpy as np
import pandas as pd
import seaborn as sns

from itertools import chain, combinations


def get_combinations(names):
    """Generates all combinations of elements from the given list of names.

    Args:
        names (list): A list of names.

    Returns:
        list: A list containing all combinations of names, ranging from pairs to the entire list.

    Examples:
        >>> get_combinations(['A', 'B', 'C'])
        [('A', 'B'), ('A', 'C'), ('B', 'C'), ('A', 'B', 'C')]

        >>> get_combinations(['X', 'Y', 'Z'])
        [('X', 'Y'), ('X', 'Z'), ('Y', 'Z'), ('X', 'Y', 'Z')]
    """
    return list(
        chain.from_iterable(combinations(names, r) for r in range(2, len(names) + 1))
    )


def create_all_dates(
    instances: list,
    instances_name: str,
    start_date: np.datetime64,
    end_date: np.datetime64,
):
    """Generate all possible combinations of instances-datetimes (hours) for a given time period.

    Args:
        instances (list): List of all instances.
        instances_name (str): Name of instances types (segment, instance_id...).
        start_date(np.datetime64): Start datetime of the period.
        end_date(np.datetime64): End datetime of the period.

    Returns:
        pd.DataFrame: A dataframe with all possible combinations per hour.

    Examples:
        >>> all_dates = create_all_dates(['a','b'], 'instance', pd.to_datetime('2023-09-11 08:00:00'), pd.to_datetime('2023-09-12 10:00:00'))
        >>> isinstance(all_dates, pd.DataFrame)
        True
        >>> set(all_dates.columns) == set(['date','instance_id','hour'])
        True
        >>> len(all_dates) == 2*(24+2+1)
        True
    """
    period = pd.date_range(start=start_date, end=end_date, freq="H")
    instances_hour = pd.DataFrame(
        {
            "date": list(np.repeat(list(period), len(instances), axis=0)),
            instances_name: instances * len(period),
        }
    )
    instances_hour["hour"] = instances_hour["date"].dt.hour
    return instances_hour


class DataAvailabilityMapper:
    """Create graphical representations of the presence or absence of data over a given time period.
    This class makes it easier to identify gaps or periods with missing data.

    Args:
        enriched_data (pd.DataFrame): Enriched Dataframe from EnrichedDataLoader.
    """

    def __init__(self, enriched_data: pd.DataFrame):
        """Initializes an instance of the APIFetcher class."""
        self.enriched_data = enriched_data

    def availability(self, level_day: bool = False):
        """
        Create a dataframe with a row by segment-hour for the entire period.
        When the segment wasn't running during this hour, value of uptime is set to 0.

        Args:
            level_day (bool): True for data at a day level, False for hour level.

        Returns:
            pd.DataFrame: A dataframe with all possible combinations per hour.
        """
        data = pd.DataFrame(
            self.enriched_data.groupby(["segment_fullname", "date"])["uptime"].max()
        ).reset_index()
        dates_ref = create_all_dates(
            list(
                set(data["segment_fullname"])
            ),  # pylint: disable=unsubscriptable-object
            "segment_fullname",
            min(data["date"]),  # pylint: disable=unsubscriptable-object
            max(data["date"]),  # pylint: disable=unsubscriptable-object
        )
        # Merge references with available data : missing days will have Nan in Uptime column
        availability = dates_ref.merge(
            data, on=["date", "segment_fullname"], how="left"
        )
        availability["uptime"] = availability["uptime"].fillna(0)
        availability["day"] = availability["date"].dt.date
        if level_day:
            availability = (
                availability.groupby(["day", "segment_fullname"])["uptime"]
                .sum()
                .reset_index()
            )
        return availability

    def heatmap_availability(self):
        """
        Visual representation of available data, with a color gradient on the uptime.
        """
        availability_heatmap_format = self.availability().pivot(
            index="segment_fullname", columns="date", values="uptime"
        )
        sns.heatmap(
            data=availability_heatmap_format, vmin=0, vmax=1, center=0.5, cmap="inferno"
        )

    def evolution_sum_uptime(self, segment_list: list = []):
        """
        Visual representation of the evolution of the sum of uptimes.
        Can be represented for all or particular sensors.
        Uptimes can be considered as a quality indicator.
        A higher value for a day may indicate :
        - a greater number of operational sensors,
        - a longer duration of daylight, or better data quality, all else being equal.

        Args:
           segment_list (list): A list of segments full names.
        """
        availables_dates = self.availability()
        title = "Sum of uptimes for all sensors"
        if len(segment_list) > 0:
            availables_dates = availables_dates[
                availables_dates["segment_fullname"].isin(segment_list)
            ]
            title = f'Sum of uptimes for {",".join(segment_list)}'
        global_period = (
            availables_dates[availables_dates["uptime"] > 0.5]
            .groupby(["day"])["uptime"]
            .sum()
            .reset_index()
        )
        sns.lineplot(data=global_period, x="day", y="uptime").set(title=title)

    def __available_segments_by_day(self, uptime_threshold: float = 0):
        """Compute available days by combinations of pair (or more) of sensors

        Args:
           uptime_threshold (float): Threshold for uptime (data lower will be removed).

        Returns:
            pd.DataFrame: A dataframe with all possible combinations per day.
        """
        segment_by_day = self.availability(level_day=True)
        segment_by_day = segment_by_day[segment_by_day["uptime"] > uptime_threshold]
        segment_by_day = (
            segment_by_day.groupby("day")["segment_fullname"].agg(list).reset_index()
        )
        segment_by_day["combinations"] = segment_by_day["segment_fullname"].apply(
            get_combinations
        )
        segment_by_day = segment_by_day[segment_by_day["combinations"].map(len) > 0]
        segment_by_day = segment_by_day.explode("combinations")
        return segment_by_day[["day", "combinations"]]

    def counter_combinations(self, uptime_threshold: float = 0):
        """Count how many days (consecutives or not) are available for each combination.

        Args:
           uptime_threshold (float): Threshold for uptime (data lower will be removed).

        Returns:
            pd.DataFrame: Dataframe with each combinations, count of available days and length of combination.
        """
        segment_by_day = self.__available_segments_by_day(uptime_threshold)
        combinations = (
            segment_by_day.groupby("combinations")
            .size()
            .reset_index(name="available_days")
        )
        combinations["combination_length"] = combinations["combinations"].apply(len)
        return combinations

    def best_combinations(self, uptime_threshold: float = 0):
        """Select, for each combination length (pairs, trios, etc.) the best combinations.
        The best combination is the one the higher number of available days.

        Args:
           uptime_threshold (float): Threshold for uptime (data lower will be removed).

        Returns:
            pd.DataFrame: Dataframe with length of combination, best combination and its number of days.
        """
        combinations = self.counter_combinations(uptime_threshold)
        idx_max = combinations.groupby("combination_length")["available_days"].idxmax()
        top_combinations = combinations.loc[idx_max].reset_index(drop=True)
        return top_combinations

    def best_combinations_details(
        self, combination_length: int, uptime_threshold: float = 0
    ):
        """Give all data for the best combination of sensors of a specified length.

        Args:
            combination_length (int) : Length of the combination wanted. The best will be choosen.
            uptime_threshold (float): Threshold for uptime (data lower will be removed).

        Returns:
            pd.DataFrame: Dataframe with traffic details (one row per day and per sensor).
        """
        # Get combination
        top_combinations = self.best_combinations(uptime_threshold)
        segments_list = top_combinations[
            top_combinations["combination_length"] == combination_length
        ]["combinations"].values[0]

        # Get list of available days
        segments_by_day = self.__available_segments_by_day(uptime_threshold)
        days_list = segments_by_day[
            segments_by_day["combinations"] == segments_list
        ]["day"].to_list()

        # Filter dataframe with these informations
        top_enriched_data = self.enriched_data[
            (self.enriched_data["segment_fullname"].isin(segments_list))
            & (self.enriched_data["day"].isin(days_list))
        ]

        return top_enriched_data
