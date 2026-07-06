from dataclasses import dataclass
from typing import Any


@dataclass
class DeviceConfig:
    device_id: str = ""
    device_title: str = ""
    device_ip: str = ""
    inner_mqtt_client_id: str = ""
    inner_mqtt_pubprefix: str = ""
    inner_mqtt_subprefix: str = ""
    serial_number: str = ""
    mqtt_enable: bool = False

    def get(self, field_name: str, def_val: Any = None):
        return getattr(self, field_name, def_val)


@dataclass
class DeviceParam:
    code: int
    data_type: int
    value: str
