import yaml
from yaml import SafeLoader


def get_yaml_infos(path: chr, key: chr):
    """Get specific informations for a YAML file

    Args:
        path (chr): path of YAML file
        key (chr): key of element in YAML file

    Returns:
        str: value of element with this key
    """
    with open(path) as f:
        data = yaml.load(f, Loader=SafeLoader)
    return data[key]
