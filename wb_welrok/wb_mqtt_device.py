import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable

from wb_welrok import config, wbmqtt
from wb_welrok.mqtt_client import MQTTClient

if TYPE_CHECKING:
    from wb_welrok.wb_welrok_device import WelrokDevice

logger = logging.getLogger(__name__)


class MQTTDevice:
    def __init__(self, mqtt_client: MQTTClient, device_state, welrok_device: "WelrokDevice"):
        self._client = mqtt_client
        self._root_topic = None
        self._device_state = device_state
        self._loop = asyncio.get_running_loop()
        logger.debug("MQTT WB device created")
        self._welrok_device = welrok_device
        self._root_topic = "/devices/" + self._welrok_device.title
        self._on_message_power = self.create_message_handler(
            self._welrok_device.set_power,
            lambda payload, _: (0 if payload.decode("utf-8") == "1" else 1,),
            "power",
        )
        self._on_message_temperature = self.create_message_handler(
            self._welrok_device.set_temp, lambda payload, _: (int(payload.decode("utf-8")),), "temperature"
        )
        self._on_message_bright = self.create_message_handler(
            self._welrok_device.set_bright,
            lambda payload, _: (
                self._welrok_device._data_parser.format_bright(int(payload.decode("utf-8"))),
            ),
            "bright",
        )
        self._on_message_mode = self.create_message_handler(
            self._welrok_device.set_mode, lambda payload, topic: (payload.decode("utf-8"), topic), "mode"
        )
        self._device = wbmqtt.Device(
            mqtt_client=self._client,
            device_mqtt_name=self._welrok_device.id,
            device_title=self._welrok_device.title,
            driver_name="wb-mqtt-welrok",
        )
        self._publicate()

    def __repr__(self):
        return (
            f"MQTTDevice(client={self._client}, device={self._device}, "
            f"welrok_device={self._welrok_device}, root_topic={self._root_topic}, "
            f"device_state={self._device_state})"
        )

    def _publicate(self) -> None:
        self._create_controls()
        logger.info("%s device created", self._root_topic)

    def create_message_handler(
        self, command: Callable, transform: Callable[[bytes, str], tuple], log_msg: str
    ):
        def handler(_, __, msg):
            try:
                args = transform(msg.payload, msg.topic)
            except Exception:
                logger.exception("Failed to decode payload for %s", log_msg)
                return
            try:
                self.send_command_to_device(command, args, log_msg)
                logger.info("%s set to %s on Welrok %s", log_msg, args, self._welrok_device.sn)
            except RuntimeError:
                logger.warning("Cannot schedule command, event loop closed")

        return handler

    def create_control(self, name, initial_value, callback=None):
        meta = wbmqtt.ControlMeta(**config.CONTROLS_CONFIG[name]["meta"])
        self._device.create_control(name, meta, initial_value)
        if callback:
            self._device.add_control_message_callback(name, callback)

    def _create_controls(self):
        power_value = self._device_state.get("powerOff", 0)
        self.create_control("Power", power_value, self._on_message_power)

        bright_val = int(self._device_state.get("bright", 9))
        start_bright = str(min(bright_val * 10, 100))
        self.create_control("Bright", start_bright, self._on_message_bright)

        temp_value = self._device_state.get("setTemp", 20)
        self.create_control("Set temperature", temp_value, self._on_message_temperature)

        modes_cfg = config.CONTROLS_CONFIG["Modes"]
        order_start = modes_cfg["order_start"]
        for idx, mode_title in enumerate(config.MODE_CODES.values()):
            meta = dict(modes_cfg["meta_template"])
            meta.update(
                {
                    "title": f'Установить режим работы "{config.MODE_NAMES_TRANSLATE.get(mode_title, mode_title)}"',
                    "title_en": f'Set mode "{mode_title}"',
                    "order": order_start + idx,
                }
            )
            meta_obj = wbmqtt.ControlMeta(**meta)
            self._device.create_control(mode_title, meta_obj, "1")
            self._device.add_control_message_callback(mode_title, self._on_message_mode)

        readonly_cfg = config.CONTROLS_CONFIG["Readonly"]
        base_order = 4 + len(config.MODE_CODES)

        load_meta = dict(readonly_cfg["Load"]["meta"])
        load_meta["order"] = base_order
        load_value = self._device_state.get("load", "Выключено")
        self._device.create_control("Load", wbmqtt.ControlMeta(**load_meta), load_value)

        current_mode_meta = dict(readonly_cfg["Current mode"]["meta"])
        current_mode_meta["order"] = base_order + 1
        current_mode_value = config.MODE_NAMES_TRANSLATE.get(self._device_state.get("mode"), "Ручной")
        self._device.create_control(
            "Current mode", wbmqtt.ControlMeta(**current_mode_meta), current_mode_value
        )

        temps_cfg = readonly_cfg["Temps"]
        temps_order_start = base_order + 2
        for idx, read_only_temp in enumerate(self._device_state.get("read_only_temp", {})):
            meta = dict(temps_cfg["meta_template"])
            meta.update(
                {
                    "title": config.TOPIC_NAMES_TRANSLATE.get(read_only_temp, read_only_temp),
                    "title_en": read_only_temp,
                    "order": temps_order_start + idx,
                }
            )
            meta_obj = wbmqtt.ControlMeta(**meta)
            value = self._device_state["read_only_temp"][read_only_temp]
            self._device.create_control(read_only_temp, meta_obj, value)

    def update(self, control_name: str, value) -> None:
        if self._device:
            self._device.set_control_value(control_name, value)
            logger.debug("%s %s control updated with value %s", self._welrok_device.id, control_name, value)

    def set_readonly(self, control_name: str, value) -> None:
        try:
            if self._device:
                self._device.set_control_read_only(control_name, True)
                self._device.set_control_value(control_name, value)
        except Exception:
            logger.exception(
                "Failed to set readonly/value for %s on %s", control_name, self._welrok_device.id
            )

    def update_temp_limits(self, min_temp: int, max_temp: int) -> None:
        if self._device:
            self._device.set_control_limits("Set temperature", min_temp, max_temp)

    def set_error_state(self, error: bool):
        for control_name in self._device.get_controls_list():
            if control_name != "IP address":
                self._device.set_control_error(control_name, "r" if error else "")

    def has_control(self, control_name: str) -> bool:
        return self._device is not None and control_name in self._device.get_controls_list()

    def ensure_temp_control(self, control_name: str, value) -> None:
        if self.has_control(control_name):
            return
        temps_cfg = config.CONTROLS_CONFIG["Readonly"]["Temps"]
        meta = dict(temps_cfg["meta_template"])
        meta.update(
            {
                "title": config.TOPIC_NAMES_TRANSLATE.get(control_name, control_name),
                "title_en": control_name,
                "order": len(self._device.get_controls_list()) + 1,
            }
        )
        self._device.create_control(control_name, wbmqtt.ControlMeta(**meta), value)
        logger.debug("Dynamically created temp control %s for %s", control_name, self._welrok_device.id)

    def set_control_error_state(self, control_name: str, error_text: str) -> None:
        if self._device:
            self._device.set_control_error(control_name, error_text)

    def remove(self) -> None:
        if self._device:
            self._device.remove_device()
            logger.info("%s device deleted", self._root_topic)

    def _done(self, f):
        try:
            f.result()
        except asyncio.CancelledError:
            logger.debug("Set operation cancelled")
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                logger.debug("Event loop closed during callback")
            else:
                logger.exception("RuntimeError in callback")
        except Exception:
            logger.exception("Exception while executing MQTT set operation")

    def send_command_to_device(self, command: Callable, cmd_args: tuple[Any], log_msg):
        if self._loop.is_closed():
            logger.warning("Event loop closed, ignoring power command")
            return
        fut = asyncio.run_coroutine_threadsafe(command(*cmd_args), self._loop)
        fut.add_done_callback(self._done)
