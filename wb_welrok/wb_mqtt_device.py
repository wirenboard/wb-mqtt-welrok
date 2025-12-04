import asyncio
import logging

from wb_welrok import wbmqtt

from wb_welrok import config
from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s (%(filename)s:%(lineno)d)")
logger.setLevel(logging.INFO)


class MQTTDevice:
    def __init__(self, mqtt_client: MQTTClient, device_state):
        self._client = mqtt_client
        self._device = None
        self._welrok_device = None
        self._root_topic = None
        self._device_state = device_state
        self._loop = asyncio.get_running_loop()
        logger.debug("MQTT WB device created")

    def __repr__(self):
        return (
            f"MQTTDevice(client={self._client}, device={self._device}, "
            f"welrok_device={self._welrok_device}, root_topic={self._root_topic}, "
            f"device_state={self._device_state})"
        )

    def set_welrok_device(self, welrok_device):
        self._welrok_device = welrok_device
        self._root_topic = "/devices/" + self._welrok_device.title
        logger.debug("Set Welrok device %s on %s topic", self._welrok_device.sn, self._root_topic)

    def publicate(self):
        self._device = wbmqtt.Device(
            mqtt_client=self._client,
            device_mqtt_name=self._welrok_device.id,
            device_title=self._welrok_device.title,
            driver_name="wb-mqtt-welrok",
        )
        self._device.create_control(
            "Power",
            wbmqtt.ControlMeta(
                title="Включение / выключение",
                title_en="Power",
                control_type="switch",
                order=1,
                read_only=False,
            ),
            self._device_state["powerOff"],
        )
        self._device.add_control_message_callback("Power", self._on_message_power)

        start_bright = (
            str(int(self._device_state["bright"]) * 10) if int(self._device_state["bright"]) != 9 else "100"
        )
        self._device.create_control(
            "Bright",
            wbmqtt.ControlMeta(
                title="Яркость дисплея",
                title_en="Display Brightness",
                units="%",
                control_type="range",
                order=2,
                read_only=False,
                max_value=100,
            ),
            start_bright,
        )
        self._device.add_control_message_callback("Bright", self._on_message_bright)

        self._device.create_control(
            "Set temperature",
            wbmqtt.ControlMeta(
                title="Установка",
                title_en="Set floor temperature",
                units="deg C",
                control_type="range",
                order=3,
                read_only=False,
                min_value=5,
                max_value=45,
            ),
            self._device_state["setTemp"],
        )
        self._device.add_control_message_callback("Set temperature", self._on_message_temperature)

        self._device.create_control(
            "Current mode",
            wbmqtt.ControlMeta(
                title="Текущий режим работы",
                title_en="Current mode",
                control_type="text",
                order=4,
                read_only=True,
            ),
            self._device_state["mode"],
        )

        self._device.create_control(
            "Load",
            wbmqtt.ControlMeta(title="Нагрев", title_en="Load", control_type="text", order=5, read_only=True),
            self._device_state["load"],
        )

        self._device.create_control(
            "Set temperature value",
            wbmqtt.ControlMeta(
                title="Установка",
                title_en="Set floor temperature",
                control_type="temperature",
                order=6,
                read_only=False,
                min_value=5,
                max_value=45,
            ),
            self._device_state["setTemp"],
        )
        self._device.add_control_message_callback("Set temperature value", self._on_message_temperature_value)

        for order_number, mode_title in enumerate(config.MODE_CODES.values(), 7):
            self._device.create_control(
                mode_title,
                wbmqtt.ControlMeta(
                    title=f"""Установить режим работы "{config.MODE_NAMES_TRANSLATE[mode_title]}" """,
                    title_en=f"""Set mode "{mode_title}" """,
                    control_type="pushbutton",
                    order=order_number,
                    read_only=False,
                ),
                "1",
            )
            self._device.add_control_message_callback(mode_title, self._on_message_mode)

        for order_number, read_only_temp in enumerate(
            self._device_state["read_only_temp"], 7 + len(config.MODE_CODES)
        ):
            self._device.create_control(
                read_only_temp,
                wbmqtt.ControlMeta(
                    title=config.TOPIC_NAMES_TRANSLATE[read_only_temp],
                    title_en=read_only_temp,
                    control_type="text",
                    order=order_number,
                    read_only=True,
                ),
                self._device_state["read_only_temp"][read_only_temp],
            )

        logger.info("%s device created", self._root_topic)

    def update(self, control_name, value):
        self._device.set_control_value(control_name, value)
        logger.debug("%s %s control updated with value %s", self._welrok_device.id, control_name, value)

    def set_readonly(self, control_name, value):
        try:
            self._device.set_control_read_only(control_name, True)
            self._device.set_control_value(control_name, value)
        except Exception:
            logger.exception(
                "Failed to set readonly/ value for %s on %s", control_name, self._welrok_device.id
            )

    def set_error_state(self, error: bool):
        for control_name in self._device.get_controls_list():
            if control_name != "IP address":
                self._device.set_control_error(control_name, "r" if error else "")

    def remove(self):
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

    def _on_message_power(self, _, __, msg):
        try:
            power = 0 if msg.payload.decode("utf-8") == "1" else 1
        except Exception:
            logger.exception("Failed to decode power payload")
            return

        try:
            if self._loop.is_closed():
                logger.warning("Event loop closed, ignoring power command")
                return
            fut = asyncio.run_coroutine_threadsafe(self._welrok_device.set_power(power), self._loop)
            fut.add_done_callback(self._done)
            logger.info("Welrok %s power state changed to %s", self._welrok_device.sn, power)
        except RuntimeError:
            logger.warning("Cannot schedule power command, event loop closed")

    def _on_message_temperature(self, _, __, msg):
        try:
            temp = int(msg.payload.decode("utf-8"))
        except Exception:
            logger.exception("Failed to decode temperature payload")
            return

        try:
            if self._loop.is_closed():
                logger.warning("Event loop closed, ignoring temperature command")
                return
            fut = asyncio.run_coroutine_threadsafe(self._welrok_device.set_temp(temp), self._loop)
            fut.add_done_callback(self._done)
            logger.info("Set temperature %s on Welrok %s", temp, self._welrok_device.sn)
        except RuntimeError:
            logger.warning("Cannot schedule temperature command, event loop closed")

    def _on_message_temperature_value(self, _, __, msg):
        try:
            temp = int(msg.payload.decode("utf-8"))
        except Exception:
            logger.exception("Failed to decode temperature value payload")
            return

        try:
            if self._loop.is_closed():
                logger.warning("Event loop closed, ignoring temperature value command")
                return
            fut = asyncio.run_coroutine_threadsafe(self._welrok_device.set_temp(temp), self._loop)
            fut.add_done_callback(self._done)
            logger.info("Set temperature %s on Welrok %s", temp, self._welrok_device.sn)
        except RuntimeError:
            logger.warning("Cannot schedule temperature value command, event loop closed")

    def _on_message_bright(self, _, __, msg):
        try:
            bright = int(msg.payload.decode("utf-8"))
        except Exception:
            logger.exception("Failed to decode bright payload")
            return
        if bright > 0:
            if bright < 10:
                bright = 1
            elif bright == 100:
                bright = 9
            else:
                bright = bright // 10

        try:
            if self._loop.is_closed():
                logger.warning("Event loop closed, ignoring bright command")
                return
            fut = asyncio.run_coroutine_threadsafe(self._welrok_device.set_bright(bright), self._loop)
            fut.add_done_callback(self._done)
            logger.info("Set bright %s on Welrok %s", bright, self._welrok_device.sn)
        except RuntimeError:
            logger.warning("Cannot schedule bright command, event loop closed")

    def _on_message_mode(self, _, __, msg):
        try:
            mode_payload = msg.payload.decode("utf-8")
        except Exception:
            logger.exception("Failed to decode mode payload")
            return

        try:
            if self._loop.is_closed():
                logger.warning("Event loop closed, ignoring mode command")
                return
            fut = asyncio.run_coroutine_threadsafe(self._welrok_device.set_mode(mode_payload), self._loop)
            fut.add_done_callback(self._done)
            logger.info("Welrok %s mode state changed to %s", self._welrok_device.sn, mode_payload)
        except RuntimeError:
            logger.warning("Cannot schedule mode command, event loop closed")
