import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
import yaml

from dotenv import load_dotenv

# Get environment variables
load_dotenv()
HEADERS = {"X-Api-Key": os.getenv("TOKEN")}
CAMERAS_URL = os.getenv("CAMERAS_URL")
REPORTS_URL = os.getenv("REPORTS_URL")
SEGMENTS_ID = os.getenv("SEGMENTS_ID").split(",")


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


def get_all_segments(time: str = "past_hour"):
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
        f"{REPORTS_URL}traffic_snapshot",
        headers=HEADERS,
        data=str(payload),
        timeout=10,
    )
    report = json2pandas(response.text, "features")
    segments = [segment["segment_id"] for segment in report["properties"]]
    return segments


def get_cameras_by_segment(segment_id: int):
    """Get all camera instances that are associated with the given segment_id
    TODO : gestion des erreurs

    Args:
        segment_id (int): Telraam id of road segment

    Returns:
        pd.DataFrame: information of all cameras of the segment
    """
    url = f"{CAMERAS_URL}/segment/{segment_id}"
    response = requests.request("GET", url, headers=HEADERS, timeout=10)
    camera = json2pandas(response.text, "camera")
    return camera


def get_active_cameras_by_segment(segment_id: int):
    """Get active cameras instances that are associated with the given segment_id
    and their version of hardware (v1 or s2).
    TODO : gestion des erreurs

    Args:
        segment_id (int): Telraam id of road segment

    Returns:
        dict: camera id and hardware version.
    """
    cameras = get_cameras_by_segment(segment_id)
    active_cameras = cameras.query("status=='active'")
    return {
        f"v{version}": instance
        for version, instance in zip(
            active_cameras["hardware_version"], active_cameras["instance_id"]
        )
    }


def get_traffic(
    telraam_id: int,
    time_start: chr,
    time_end: chr,
    level: chr = "segments",
    telraam_format: chr = "per-hour",
):
    """Get traffic informance for a sensor (instance) or a segment
    TODO : gestion des erreurs

    Args:
        telraam_id (int): Telraam id of sensor or segment
        time_start (chr): beginning of the requested time interval - YYYY-MM-DD HH:MM:SSZ format
        time_end (chr): end of the requested time interval - YYYY-MM-DD HH:MM:SSZ format
        level (chr, optional): 'instances' if traffic for a sensor. Defaults to 'segments'.
        telraam_format (chr, optional): Only per hour, per quarter soon. Defaults to 'per-hour'.

    Returns:
        pd.DataFrame : Traffic informations for this sensor/this segment during specified period
    """
    payload = {
        "id": telraam_id,
        "level": level,
        "format": telraam_format,
        "time_start": time_start,
        "time_end": time_end,
    }
    response = requests.request(
        "POST", f"{REPORTS_URL}traffic", headers=HEADERS, data=str(payload), timeout=10
    )
    report = json2pandas(response.text, "report")
    return report


def create_sensors_file():
    """Get cameras infos for segments specified in .env
    Create a YAML files with major cameras informations.
    """
    sensors = pd.DataFrame()
    info = [
        "instance_id",
        "segment_id",
        "mac",
        "hardware_version",
        "status",
        "time_added",
    ]
    for segment in SEGMENTS_ID:
        sensors_tmp = get_cameras_by_segment(segment)
        sensors = pd.concat([sensors, sensors_tmp[info]], ignore_index=True)
    with open("config/sensors.yaml", "w", encoding="utf-8") as file:
        yaml.dump(sensors.to_dict("index"), file, default_flow_style=False)


# get_traffic(7785, "2023-10-01 12:00:00Z", "2023-10-01 13:00:00Z", level="instances")
# get_traffic(9000002156, "2023-10-01 12:00:00Z", "2023-10-01 13:00:00Z")
