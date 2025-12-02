from typing import Callable

CONFIG_FILEPATH = "/etc/wb-welrok.conf"
SCHEMA_FILEPATH = "/usr/share/wb-mqtt-confed/schemas/wb-mqtt-welrok.schema.json"

data_topics = ["floorTemp", "airTemp", "protTemp", "setTemp", "powerOff", "load"]
settings_topics = ["setTemp", "bright", "powerOff", "mode"]

TEMP_CODES = {0: "Overheat temperature", 1: "Floor temperature", 2: "Air temperature", 7: "MK temperature"}

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

CMD_CODES = {"params": 1, "telemetry": 4}

PARAMS_CODES = {125: "powerOff", 23: "bright", 31: "setTemp", 2: "mode"}

MODE_CODES = {
    0: "Auto",
    1: "Manual",
}

MODE_NAMES_TRANSLATE = {"Auto": "По расписанию", "Manual": "Ручной"}

MODE_CODES_REVERSE = {
    "Auto": 0,
    "Manual": 1,
}

PARAMS_CHOISE: dict[str, Callable] = {
    "powerOff": lambda x: "1" if x == "0" else "0",
    "bright": lambda x: round(float(x), 2),
    "setTemp": lambda x: round(float(x), 2),
    "mode": lambda x: MODE_CODES[int(x)],
    "load": lambda x: "Выключено" if x == "0" else "Включено",
}
