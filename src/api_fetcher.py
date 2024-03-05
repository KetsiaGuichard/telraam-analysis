import json
import os
import time
from abc import ABC
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm
import requests
import yaml
import pandas as pd

# Get environment variables
load_dotenv()


def json2pandas(json_text: chr, key: chr):
    """Convert Telraam API response to pandas df

    Args:
        json_text (chr): Telraam API response, JSON format
        key (chr): key of informations to put in pd dataframe

    Returns:
        pd.DataFrame: Value informations for this key in a dataframe
    """
    dict_json = json.loads(json_text)
    if len(key)==0:
        return pd.DataFrame.from_dict(dict_json)
    return pd.DataFrame.from_dict(dict_json[key])


def check_lt_3months(time_start: chr, time_end: chr):
    """Check if start and end dates are separated by less than 90 days.

    Args:
        time_start (chr): Start time, YYYY-MM-DD HH:MM:SSZ format
        time_end (chr): End time, YYYY-MM-DD HH:MM:SSZ format

    Returns:
        bool: True if the period is less or equal to 90 days, False otherwise

    Examples:
        >>> check_lt_3months('2000-01-01 06:00:00Z','2000-03-01 06:00:00Z')
        True
        >>> check_lt_3months('2000-01-01 06:00:00Z','2000-03-31 06:00:00Z')
        True
        >>> check_lt_3months('2000-01-01 06:00:00Z','2000-04-01 06:00:00Z')
        False
    """
    start = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%SZ")
    end = datetime.strptime(time_end, "%Y-%m-%d %H:%M:%SZ")
    diff = (end - start).days
    if diff > 90:
        return False
    return True


def divide_into_subperiods(time_start: chr, time_end: chr, subperiod=90):
    """Divide a period into fixed subperiod with a determined number of days.

    Args:
        time_start (chr): Start time, YYYY-MM-DD HH:MM:SSZ format
        time_end (chr): End time, YYYY-MM-DD HH:MM:SSZ format
        subperiod (int): Number of days of subperiods, default is 90 days.

    Returns:
        list(list(chr)): [[start_period1, end_period1], ...]

    Examples:
        >>> divide_into_subperiods('2000-01-01 06:00:00Z','2000-03-31 06:00:00Z', subperiod=90)
        [['2000-01-01 06:00:00Z', '2000-03-31 06:00:00Z']]
        >>> divide_into_subperiods('2000-01-01 06:00:00Z', '2000-04-01 06:00:00Z', subperiod=90)
        [['2000-01-01 06:00:00Z', '2000-03-31 06:00:00Z'], ['2000-03-31 07:00:00Z', '2000-04-01 06:00:00Z']]
    """

    start = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%SZ")
    end = datetime.strptime(time_end, "%Y-%m-%d %H:%M:%SZ")
    list_subperiods = []
    begin_subperiod = start
    while begin_subperiod < end:
        theorical_end = begin_subperiod + timedelta(days=subperiod)
        end_subperiod = min(theorical_end, end)
        list_subperiods.append(
            [
                date.strftime("%Y-%m-%d %H:%M:%SZ")
                for date in [begin_subperiod, end_subperiod]
            ]
        )
        begin_subperiod = end_subperiod + timedelta(hours=1)
    return list_subperiods


class APIFetcher(ABC):
    """Retrieve Telraam data from its API."""

    def __init__(
        self,
    ):
        """Initializes an instance of the APIFetcher class."""
        self.header = {"X-Api-Key": os.getenv("TOKEN")}
        self.cameras_url = os.getenv("CAMERAS_URL")
        self.reports_url = os.getenv("REPORTS_URL")
        self.segments_id = list(map(int, os.getenv("SEGMENTS_ID").split(",")))
        self.instances_id = list(map(int, os.getenv("INSTANCES_ID").split(",")))


class SystemFetcher(APIFetcher):
    """Retrieve Telraam info on a system : get active cameras, all segments in world, etc."""

    def get_all_segments(self, period: str = "past_hour"):
        """Get id's of all active segments for specified time

        Args:
            time (str, optional): Asking time (format "%Y-%m-%d %H:00:00Z"). Defaults to past hour.

        Returns:
            list: list of all active segments for this time
        """
        if period == "past_hour":
            past_hour = datetime.now() - timedelta(hours=3)
            period = past_hour.strftime("%Y-%m-%d %H:00:00Z")
        payload = {"time": period, "contents": "minimal", "area": "full"}
        response = requests.request(
            "POST",
            f"{self.reports_url}traffic_snapshot",
            headers=self.header,
            data=str(payload),
            timeout=20,
        )
        report = json2pandas(response.text, "features")
        response.raise_for_status()
        segments = [segment["segment_id"] for segment in report["properties"]]
        return segments

    def get_all_cameras(self):
        """Get id's of all cameras

        Returns:
            pd.DataFrame: list of all active cameras with their segment and version
        """
        response = requests.request(
            "GET", self.cameras_url, headers=self.header, timeout=10
        )
        response.raise_for_status()
        report = json2pandas(response.text, "cameras")
        cameras = report.query('status == "active"').filter(
            ["instance_id", "segment_id", "hardware_version"]
        )
        time.sleep(5)
        return cameras.reset_index()

    def get_cameras_by_segment(self, segment_id: int):
        """Get all camera instances that are associated with the given segment_id

        Args:
            segment_id (int): Telraam id of road segment

        Returns:
            pd.DataFrame: information of all cameras of the segment
        """
        url = f"{self.cameras_url}/segment/{segment_id}"
        response = requests.request("GET", url, headers=self.header, timeout=20)
        response.raise_for_status()
        camera = json2pandas(response.text, "camera")
        time.sleep(5)
        return camera

    def get_active_cameras_by_segment(self, segment_id: int):
        """Get active cameras instances that are associated with the given segment_id,
        their version of hardware (v1 or s2) and the time they were installed.

        Args:
            segment_id (int): Telraam id of road segment

        Returns:
            dict: camera id : hardware version and installation date.
        """
        cameras = self.get_cameras_by_segment(segment_id)
        if not isinstance(cameras, int):
            active_cameras = cameras.query("status=='active'")
            return {
                f"v{version}": {"id": instance, "time_added": time_added}
                for version, instance, time_added in zip(
                    active_cameras["hardware_version"],
                    active_cameras["instance_id"],
                    active_cameras["time_added"],
                )
            }
        return cameras

    def sensors_informations(self, write=False):
        """Get cameras infos for segments specified in .env
        Create a YAML files with major cameras informations if write=True.

        Args:
            write (bool): Default to False (no writting in config file)

        Returns:
            pd.DataFrame: Dataframe of all sensors with information.
        """
        sensors = pd.DataFrame()
        for segment in tqdm(self.segments_id):
            sensors_tmp = self.get_cameras_by_segment(segment)
            sensors = pd.concat([sensors, sensors_tmp], ignore_index=True)
        if write:
            with open("config/sensors.yaml", "w", encoding="utf-8") as file:
                yaml.dump(sensors.to_dict("index"), file, default_flow_style=False)
        return sensors


class TrafficFetcher(APIFetcher):
    """Initializes an instance of the APIFetcher class.

    Args:
        time_start (chr): beginning of the requested time interval - YYYY-MM-DD HH:MM:SSZ format
        time_end (chr): end of the requested time interval - YYYY-MM-DD HH:MM:SSZ format
        level (chr, optional): 'instances' if traffic for a sensor. Defaults to 'segments'.
        telraam_format (chr, optional): Only per hour, per quarter soon. Defaults to 'per-hour'.
    """

    def __init__(
        self,
        time_start: chr,
        time_end: chr,
        level: chr = "segments",
        telraam_format: chr = "per-hour",
    ):
        super().__init__()
        self.time_start = time_start
        self.time_end = time_end
        self.level = level
        self.telraam_format = telraam_format
        self.periods = divide_into_subperiods(self.time_start, self.time_end)

    def get_traffic(self, telraam_id):
        """Get traffic informance for a sensor (instance) or a segment

        Args:
            telraam_id (int): Telraam id of sensor or segment

        Returns:
            pd.DataFrame : Traffic informations for this sensor/this segment during specified period

        """
        report = pd.DataFrame()
        columns = 'instance_id,segment_id,date,interval,uptime,heavy,car,bike,pedestrian,heavy_lft,heavy_rgt,car_lft,car_rgt,bike_lft,bike_rgt,pedestrian_lft,pedestrian_rgt,direction,car_speed_hist_0to120plus,mode_bicycle_lft,mode_bicycle_rgt,mode_bus_lft,mode_bus_rgt,mode_car_lft,mode_car_rgt,mode_lighttruck_lft,mode_lighttruck_rgt,mode_motorcycle_lft,mode_motorcycle_rgt,mode_pedestrian_lft,mode_pedestrian_rgt,mode_stroller_lft,mode_stroller_rgt,mode_tractor_lft,mode_tractor_rgt,mode_trailer_lft,mode_trailer_rgt,mode_truck_lft,mode_truck_rgt,speed_hist_car_lft,speed_hist_car_rgt,v85'
        for period in self.periods:
            payload = {
                "id": telraam_id,
                "level": self.level,
                "format": self.telraam_format,
                "time_start": period[0],
                "time_end": period[1],
                "columns": columns,
            }
            response = requests.request(
                "POST",
                f"{self.reports_url}traffic",
                headers=self.header,
                data=str(payload),
                timeout=30,
            )
            response.raise_for_status()
            time.sleep(5)
            report_tmp = json2pandas(response.text, "report")
            report = pd.concat([report, report_tmp], ignore_index=True)
        return report

    def get_all_traffic(self, waiting_time: int = 10):
        """Get traffic for given dates and all segments in config file.
        If level = 'instances' and the segment has multiple cameras, each camera will be included.
        Otherwise (level = 'segments', default), only the main camera will be included.

        Args:
            waiting_time (int, optional): Waiting time between 2 requests (seconds). Defaults to 10.

        Returns:
            pd.DataFrame: Traffic Data
        """
        traffic = pd.DataFrame()

        if self.level == "segments":
            telraam_ids = self.segments_id
        elif self.level == "instances":
            telraam_ids = self.instances_id

        for tmp_id in tqdm(telraam_ids):
            traffic_tmp = self.get_traffic(tmp_id)
            if not traffic_tmp.empty:
                traffic = pd.concat([traffic, traffic_tmp], ignore_index=True)
        return traffic
