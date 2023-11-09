import json
import os
import pandas as pd
import requests
import time
import yaml

from abc import ABC
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

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
    return pd.DataFrame.from_dict(dict_json[key])


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
            "GET", self.cameras_url, headers=self.header, timeout=20
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
        response = requests.request("GET", url, headers=self.header, timeout=10)
        response.raise_for_status()
        camera = json2pandas(response.text, "camera")
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

    def create_sensors_file(self):
        """Get cameras infos for segments specified in .env
        Create a YAML files with major cameras informations.
        """
        sensors = pd.DataFrame()
        for segment in tqdm(self.segments_id):
            sensors_tmp = self.get_cameras_by_segment(segment)
            sensors = pd.concat([sensors, sensors_tmp], ignore_index=True)
        with open("config/sensors.yaml", "w", encoding="utf-8") as file:
            yaml.dump(sensors.to_dict("index"), file, default_flow_style=False)


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

    def get_traffic(self, telraam_id):
        """Get traffic informance for a sensor (instance) or a segment

        Args:
            telraam_id (int): Telraam id of sensor or segment

        Returns:
            pd.DataFrame : Traffic informations for this sensor/this segment during specified period

        #TODO : générer erreur si période de temps supérieure à 3 mois
        """
        payload = {
            "id": telraam_id,
            "level": self.level,
            "format": self.telraam_format,
            "time_start": self.time_start,
            "time_end": self.time_end,
        }
        response = requests.request(
            "POST",
            f"{self.reports_url}traffic",
            headers=self.header,
            data=str(payload),
            timeout=10,
        )
        response.raise_for_status()
        report = json2pandas(response.text, "report")
        return report

    def get_all_traffic(self, waiting_time: int = 10):
        """Get traffic for given dates and all segments in config file.
        If level = 'instances' and the segment has multiple cameras, each camera will be included.
        Otherwise (level = 'segments', default), only the main camera will be included.

        Args:
            waiting_time (int, optional): Waiting time between two requests (in seconds). Defaults to 10.

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
            time.sleep(waiting_time)
        return traffic