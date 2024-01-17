import numpy as np
import pandas as pd
import seaborn as sns


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

    def availability(self):
        """
        Create a dataframe with a row by segment-hour for the entire period.
        When the segment wasn't running during this hour, value of uptime is set to 0.

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
