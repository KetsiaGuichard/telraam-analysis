from abc import ABC
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
import yaml


from dotenv import load_dotenv

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
        self.segments_id = os.getenv("SEGMENTS_ID").split(",")
        self.cameras_url = os.getenv("CAMERAS_URL")
        self.reports_url = os.getenv("REPORTS_URL")


class SystemFetcher(APIFetcher):
    """Retrieve Telraam info on a system : get active cameras, all segments in world, etc."""

    def get_all_segments(self, time: str = "past_hour"):
        """Get id's of all active segments for specified time

        Args:
            time (str, optional): Asking time (format "%Y-%m-%d %H:00:00Z"). Defaults to past hour.

        Returns:
            list: list of all active segments for this time
        """
        if time == "past_hour":
            past_hour = datetime.now() - timedelta(hours=3)
            time = past_hour.strftime("%Y-%m-%d %H:00:00Z")
        payload = {"time": time, "contents": "minimal", "area": "full"}
        response = requests.request(
            "POST",
            f"{self.reports_url}traffic_snapshot",
            headers=self.header,
            data=str(payload),
            timeout=20,
        )
        if response.status_code == 200:
            report = json2pandas(response.text, "features")
            segments = [segment["segment_id"] for segment in report["properties"]]
            return segments
        return None

    def get_all_cameras(self):
        """Get id's of all cameras

        Returns:
            pd.DataFrame: list of all active cameras with their segment and version
        """
        response = requests.request(
            "GET", self.cameras_url, headers=self.header, timeout=20
        )
        if response.status_code == 200:
            report = json2pandas(response.text, "cameras")
            cameras = report.query('status == "active"').filter(
                ["instance_id", "segment_id", "hardware_version"]
            )
            return cameras.reset_index()
        return None

    def get_cameras_by_segment(self, segment_id: int):
        """Get all camera instances that are associated with the given segment_id

        Args:
            segment_id (int): Telraam id of road segment

        Returns:
            pd.DataFrame: information of all cameras of the segment
        """
        url = f"{self.cameras_url}/segment/{segment_id}"
        response = requests.request("GET", url, headers=self.header, timeout=10)
        if response.status_code == 200:
            camera = json2pandas(response.text, "camera")
            return camera
        return response.status_code

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
        for segment in self.segments_id:
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
        if response.status_code == 200:
            report = json2pandas(response.text, "report")
            return report
        return None

    def get_full_traffic(self):
        pass