from enum import Enum
from typing import Callable

CONFIG_FILEPATH = "/etc/wb-welrok.conf"
SCHEMA_FILEPATH = "/usr/share/wb-mqtt-confed/schemas/wb-mqtt-welrok.schema.json"
DEFAULT_BROKER_UNIX = "unix:///var/run/mosquitto/mosquitto.sock"
DEFAULT_BROKER_URL = "tcp://127.0.0.1:1883"

MQTT_PUBLISH_TIMEOUT = 5
HTTP_REREQUEST_TIMEOUT = 20
HTTP_FAILURE_THRESHOLD = 3
HTTP_INIT_RETRIES = 3
HTTP_PERIODIC_RETRIES = 1
HTTP_REQUEST_TIMEOUT = 20

data_topics = ["floorTemp", "airTemp", "protTemp", "setTemp", "powerOff", "load"]
settings_topics = ["setTemp", "bright", "powerOff", "mode"]

TEMP_CODES = {
    0: "Overheat temperature",
    1: "Floor temperature",
    2: "Air temperature",
    7: "MK temperature",
}

TOPIC_NAMES_TRANSLATE = {
    "Overheat temperature": "Внутренняя температура устройства",
    "Floor temperature": "Температура пола",
    "Air temperature": "Температура воздуха",
    "MK temperature": "Температура процессора",
}

INNER_TOPICS = {
    "protTemp": "Overheat temperature",
    "floorTemp": "Floor temperature",
    "airTemp": "Air temperature",
    "setTemp": "Set temperature",
    "powerOff": "Power",
    "load": "Load",
}


class TemperatureCode(Enum):
    OVERHEAT = ("t.0", 16, 2, "Overheat temperature")
    FLOOR_TEMP = ("t.1", 16, 2, "Floor temperature")
    AIR_TEMP = ("t.2", 16, 2, "Air temperature")
    MK_TEMP = ("t.7", 16, 2, "MK temperature")

    def __init__(self, key, divisor, precision, title):
        self.key = key
        self.divisor = divisor
        self.precision = precision
        self.title = title


class FaultCode(Enum):
    OVERHEAT = ("f.9", "Overheat temperature", "перегрев устройства")
    OVERHEAT_SENSOR = ("f.12", "Overheat temperature", "ошибка датчика перегрева")
    FLOOR_OPEN_CIRCUIT = ("f.3", "Floor temperature", "обрыв датчика пола")
    FLOOR_SHORT_CIRCUIT = ("f.4", "Floor temperature", "КЗ датчика пола")
    AIR_SENSOR_LOSS = ("f.5", "Air temperature", "потеря датчика воздуха")
    AIR_SENSOR_BATTERY = ("f.23", "Air temperature", "низкий заряд батареи датчика воздуха")

    def __init__(self, code: str, control_title: str, error_text: str):
        self.code = code
        self.control_title = control_title
        self.error_text = error_text


class StateCode(Enum):
    LOAD = "f.0"


class CmdCode(Enum):
    PARAMS = 1
    TELEMETRY = 4


CMD_CODES = {"params": 1, "telemetry": 4}

MODE_CODES = {
    0: "Auto",
    1: "Manual",
}

PARAMS_CHOISE: dict[str, Callable] = {
    "powerOff": lambda x: "1" if x == "0" else "0",
    "bright": lambda x: round(float(x), 2),
    "setTemp": lambda x, div: round(float(x) / div, 2),
    "mode": lambda x: MODE_CODES[int(x)],
    "load": lambda x: "Выключено" if x == "0" else "Включено",
}

MODE_NAMES_TRANSLATE = {"Auto": "По расписанию", "Manual": "Ручной"}

MODE_CODES_REVERSE = {
    "Auto": 0,
    "Manual": 1,
}


# Если True: при изменении температуры в режиме расписания (Auto) режим не переключается
# в ручной — температура задаётся для текущего режима как есть.
# Если False (по умолчанию): при любом изменении температуры устройство переходит в ручной режим.
SET_TEMP_KEEP_SCHEDULE_MODE = False


class DefaultParseValue(Enum):
    TEMP_DIV = 1
    UPPER_LIMIT_TEMP = 45
    LOWER_LIMIT_TEMP = 5
    UPPER_LIMIT_BRIGHT = 10
    LOWER_LIMIT_BRIGHT = 0


class ParamCode(Enum):
    POWER = 125
    MANUAL_AIR_TEMP = 4
    MANUAL_FLOOR_TEMP = 5
    TEMP = 31
    TEMP_SCHEDULE = 29
    MODE = 2
    BRIGHT = 23
    CONTROL_TYPE = 3
    UPPER_LIMIT = 26
    LOWER_LIMIT = 27
    UPPER_AIR_LIMIT = 33
    LOWER_AIR_LIMIT = 34


PARAMS_CODES = {
    ParamCode.POWER: "powerOff",
    ParamCode.BRIGHT: "bright",
    ParamCode.TEMP: "setTemp",
    ParamCode.MODE: "mode",
}


class HttpCode(Enum):
    POWER = 7
    TEMP = 3
    MODE = 2
    BRIGHT = 2


class ModeCode(Enum):
    MANUAL = MODE_CODES_REVERSE["Manual"]


CONTROLS_CONFIG = {
    "Power": {
        "meta": {
            "title": "Включение / выключение",
            "title_en": "Power",
            "control_type": "switch",
            "order": 1,
            "read_only": False,
        },
    },
    "Bright": {
        "meta": {
            "title": "Яркость дисплея",
            "title_en": "Display Brightness",
            "units": "%",
            "control_type": "range",
            "order": 2,
            "read_only": False,
            "max_value": 100,
        },
    },
    "Set temperature": {
        "meta": {
            "title": "Установка",
            "title_en": "Set floor temperature",
            "units": "deg C",
            "control_type": "range",
            "order": 3,
            "read_only": False,
            "min_value": 5,
            "max_value": 45,
        },
    },
    "Modes": {
        "meta_template": {
            "control_type": "pushbutton",
            "read_only": False,
        },
        "order_start": 4,
    },
    "Readonly": {
        "Load": {
            "meta": {
                "title": "Нагрузка",
                "title_en": "Load",
                "control_type": "text",
                "order": None,
                "read_only": True,
            },
        },
        "Current mode": {
            "meta": {
                "title": "Текущий режим работы",
                "title_en": "Current mode",
                "control_type": "text",
                "order": None,
                "read_only": True,
            },
        },
        "Temps": {
            "meta_template": {
                "control_type": "text",
                "read_only": True,
                "units": "deg C",
            },
            "order_start": None,
        },
    },
}
