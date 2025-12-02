import argparse
import asyncio
import json
import logging
import signal
import sys
import traceback

import aiohttp
import jsonschema

from wb_welrok import config, wbmqtt
from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient

HTTP_REREQUEST_TIMEOUT = 20  # seconds
MQTT_PUBLISH_TIMEOUT = 5  # seconds

# How many attempts to try when doing the initial device discovery (startup)
HTTP_INIT_RETRIES = 3
# How many attempts to try during periodic polling loop (keep small to avoid long blocking)
HTTP_PERIODIC_RETRIES = 1
# How many consecutive failures before we escalate logging to WARNING / mark degraded
HTTP_FAILURE_THRESHOLD = 3

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


class WelrokDevice:
    def __init__(self, properties):
        self._id = properties.get("device_id", "unknown_id")
        self._title = properties.get("device_title", "unknown_title")
        self._sn = properties.get("serial_number", "unknown_sn")
        self._ip = properties.get("device_ip", "unknown_ip")
        self._mqtt_enable = properties.get("mqtt_enable", False)
        self._mqtt_server_uri = properties.get("mqtt_server_uri", DEFAULT_BROKER_URL)
        self._url = (
            f"""http://{properties.get("device_ip")}/api.cgi""" if properties.get("device_ip") else None
        )
        self._wb_mqtt_device = None
        self._mqtt = MQTTClient(self._title, self._mqtt_server_uri)
        self._mqtt.user_data_set(self)
        self._subscribed_topics = set()
        self._mqtt_pub_base_topic = f"""{properties.get("inner_mqtt_pubprefix", "")}{properties.get("inner_mqtt_client_id", "")}/set/"""
        self._mqtt_sub_base_topic = f"""{properties.get("inner_mqtt_subprefix", "")}{properties.get("inner_mqtt_client_id", "")}/get/"""
        self._mqtt_data_topics = config.data_topics
        self._mqtt_settings_topics = config.settings_topics
        self._session = None  # Singleton aiohttp session
        # Counters for consecutive failures (used to reduce noisy warnings)
        self._params_failures = 0
        self._telemetry_failures = 0

        logger.debug("Add device with id " + self._id + " and sn " + self._sn)

    def __repr__(self):
        return (
            f"WelrokDevice(id={self._id}, title={self._title}, "
            f"serial_number={self._sn}, ip={self._ip}, url={self._url}, "
            f"mqtt_pub_base_topic={self._mqtt_pub_base_topic}, "
            f"mqtt_sub_base_topic={self._mqtt_sub_base_topic}, "
            f"mqtt_data_topics={self._mqtt_data_topics}, "
            f"mqtt_settings_topics={self._mqtt_settings_topics})"
        )

    @property
    def id(self):  # pylint: disable=C0103
        return self._id

    @property
    def sn(self):
        return self._sn

    @property
    def title(self):
        return self._title

    @property
    def ip(self):
        return self._ip

    def set_mqtt_device(self, mqtt_device: MQTTDevice):
        self._wb_mqtt_device = mqtt_device
        logger.debug("Set WB MQTT device for Welrok %s", self._id)

    def _get_session(self):
        """Get or create singleton aiohttp session with timeout and connection limits"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_REREQUEST_TIMEOUT)
            connector = aiohttp.TCPConnector(
                limit_per_host=2,  # Max 2 simultaneous connections per host
                force_close=True,  # Force close connections for unstable devices
                enable_cleanup_closed=True,
                ssl=False,  # Explicitly disable SSL for HTTP connections
            )
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)
        return self._session

    async def _close_session(self):
        """Close aiohttp session if exists"""
        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
                # Wait for proper cleanup
                await asyncio.sleep(0.25)
            except Exception:
                logger.exception("Error closing aiohttp session for device %s", self._id)

    def parse_temperature_response(self, data):
        current_temp = {}
        for code, value in config.TEMP_CODES.items():
            if f"t.{code}" in data:
                v = (
                    str(round(int(data[f"t.{code}"]) / 16, 2))
                    if code == 1
                    else str(round(int(data[f"t.{code}"]) / 16))
                )
                current_temp.update({value: v})
        for code in range(3, 7):
            sensor = 1 if code in (3, 4) else 2
            if f"f.{code}" in data and data[f"f.{code}"] == "1":
                current_temp.update({config.TEMP_CODES[sensor]: "КЗ или обрыв цепи"})
        return current_temp

    def parse_response(self, raw_response, param):
        for par in raw_response["par"]:
            if par[0] == param:
                return par[2]
        return None

    def parse_device_params_state(self, data):
        state = {}
        logger.debug("Parse device params state data: %s", data)
        try:
            for par in data["par"]:
                if par[0] in config.PARAMS_CODES:
                    if config.PARAMS_CODES[par[0]] == "setTemp":
                        state.update(
                            {
                                config.PARAMS_CODES[par[0]]: config.PARAMS_CHOISE[
                                    config.PARAMS_CODES[par[0]]
                                ](par[2])
                                / 10
                            }
                        )
                    else:
                        state.update(
                            {
                                config.PARAMS_CODES[par[0]]: config.PARAMS_CHOISE[
                                    config.PARAMS_CODES[par[0]]
                                ](par[2])
                            }
                        )
            logger.debug("""Parse device params state "STATE": %s""", state)
            return state
        except Exception as e:
            logger.debug("Error: %s.\n%s", e, traceback.format_exc())
            return None

    async def get_device_state(self, cmd, retries: int = 2, retry_delay: float = 0.5):
        """Perform POST to device API with retries.

        Args:
            cmd: numeric command code to send in the payload
            retries: how many attempts (total). Default 2 — one retry.
            retry_delay: delay in seconds between attempts (exponential backoff multiplier applied)
        Returns parsed JSON on success or None on failures/timeouts.
        """
        attempt = 0
        post_data = json.dumps({"cmd": cmd})
        while attempt < retries:
            attempt += 1
            try:
                logger.debug("Start get_device_state (attempt %s/%s)", attempt, retries)
                logger.debug("Get_device_state post_data: %s", post_data)
                session = self._get_session()
                async with session.post(self._url, data=post_data) as resp:
                    status = resp.status
                    if status == 200:
                        try:
                            return await resp.json()
                        except aiohttp.ContentTypeError:
                            logger.debug("Response content is not in JSON format from %s", self._id)
                            return None
                    logger.error("HTTP error %s for device %s", status, self._id)
                    return None

            except asyncio.TimeoutError:
                logger.error(
                    "Request timeout after %s seconds (device %s, attempt %s/%s)",
                    HTTP_REREQUEST_TIMEOUT,
                    self._id,
                    attempt,
                    retries,
                )
            except (
                aiohttp.ClientConnectorError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientOSError,
                aiohttp.ClientConnectionError,
            ) as e:
                logger.debug("Network error for %s (attempt %s/%s): %s", self._id, attempt, retries, str(e))
            except Exception as e:
                logger.debug(
                    "Unexpected error during request to %s (attempt %s/%s): %s.\n%s",
                    self._id,
                    attempt,
                    retries,
                    e,
                    traceback.format_exc(),
                )

            # If we still have attempts left, sleep with slight backoff
            if attempt < retries:
                await asyncio.sleep(retry_delay * attempt)

        return None

    async def set_current_temp(self, current_temp):
        for key, value in current_temp.items():
            if "open" not in value:
                value = f"{value} \u00B0C"
            logger.debug("Welrok device %s current_temp item key = %s, value = %s", self._id, key, value)
            self._wb_mqtt_device.set_readonly(key, value)

    async def set_current_control_state(self, current_states):
        for key, value in current_states.items():
            logger.debug("Welrok device %s curent state item key = %s, value = %s", self._id, key, value)
            self._wb_mqtt_device.update(key, value)

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.warning("MQTT connect failed with rc=%s for device %s", rc, self._id)
            return

        logger.info("MQTT connected for device %s, subscribing topics", self._id)
        for topic in self._mqtt_data_topics:
            full = self._mqtt_sub_base_topic + topic
            try:
                client.message_callback_add(full, self.mqtt_data_callback)
                result, mid = client.subscribe(full, qos=1)
                if result == 0:
                    self._subscribed_topics.add(full)
                    logger.debug("Subscribed to %s (mid=%s) for %s", full, mid, self._id)
                else:
                    logger.warning("Subscribe returned %s for %s", result, full)
            except Exception:
                logger.exception("Failed to subscribe to %s for device %s", full, self._id)

    async def run(self):
        mqtt_started = False

        while True:  # Infinite retry loop for MQTT resilience
            try:
                # Start MQTT only once
                if self._mqtt is not None and not mqtt_started:
                    self._mqtt.on_connect = self._on_mqtt_connect
                    self._mqtt.on_disconnect = lambda client, userdata, rc: logger.warning(
                        "MQTT disconnected for %s, will retry", self._id
                    )
                    self._mqtt.start()
                    mqtt_started = True

                while True:  # Main polling loop
                    if self._url:
                        # For periodic polling keep retries low so the loop doesn"t block too long
                        params_response = await self.get_device_state(
                            config.CMD_CODES["params"], retries=HTTP_PERIODIC_RETRIES
                        )
                        if params_response is not None:
                            # success -> reset failures counter
                            self._params_failures = 0
                            logger.debug(params_response)
                        else:
                            # failure -> increment and cap at threshold to prevent overflow
                            self._params_failures = min(self._params_failures + 1, HTTP_FAILURE_THRESHOLD)
                            # Ensure we never exceed threshold due to bugs
                            if self._params_failures > HTTP_FAILURE_THRESHOLD:
                                self._params_failures = HTTP_FAILURE_THRESHOLD

                            if self._params_failures == HTTP_FAILURE_THRESHOLD:
                                logger.warning(
                                    "Device %s params unavailable in periodic poll (failed %s consecutive times)",
                                    self._id,
                                    self._params_failures,
                                )
                            elif self._params_failures < HTTP_FAILURE_THRESHOLD:
                                logger.debug(
                                    "Device %s transient params error (failed %s/%s)",
                                    self._id,
                                    self._params_failures,
                                    HTTP_FAILURE_THRESHOLD,
                                )
                        if params_response:
                            device_controls_state = self.parse_device_params_state(params_response) or {}
                            control_states = {
                                "Power": int(device_controls_state.get("powerOff", 0)),
                                "Bright": int(device_controls_state.get("bright", 0) * 10),
                                "Set temperature": int(device_controls_state.get("setTemp", 0)),
                                "Set temperature value": int(device_controls_state.get("setTemp", 0)),
                            }
                            if self._wb_mqtt_device:
                                self._wb_mqtt_device.set_readonly(
                                    "Current mode",
                                    config.MODE_NAMES_TRANSLATE.get(
                                        device_controls_state.get("mode", ""), ""
                                    ),
                                )
                                await self.set_current_control_state(control_states)

                        telemetry = await self.get_device_state(
                            config.CMD_CODES["telemetry"], retries=HTTP_PERIODIC_RETRIES
                        )
                        if telemetry is not None:
                            # success -> reset telemetry failures counter
                            self._telemetry_failures = 0
                            logger.debug(telemetry)
                        else:
                            # failure -> increment and cap at threshold to prevent overflow
                            self._telemetry_failures = min(
                                self._telemetry_failures + 1, HTTP_FAILURE_THRESHOLD
                            )
                            # Ensure we never exceed threshold due to bugs
                            if self._telemetry_failures > HTTP_FAILURE_THRESHOLD:
                                self._telemetry_failures = HTTP_FAILURE_THRESHOLD

                            if self._telemetry_failures == HTTP_FAILURE_THRESHOLD:
                                logger.warning(
                                    "Device %s telemetry unavailable in periodic poll (failed %s consecutive times)",
                                    self._id,
                                    self._telemetry_failures,
                                )
                            elif self._telemetry_failures < HTTP_FAILURE_THRESHOLD:
                                logger.debug(
                                    "Device %s transient telemetry error (failed %s/%s)",
                                    self._id,
                                    self._telemetry_failures,
                                    HTTP_FAILURE_THRESHOLD,
                                )
                        if telemetry:
                            self._wb_mqtt_device.set_readonly("Load", self.get_load(telemetry))
                            await self.set_current_temp(self.parse_temperature_response(telemetry))
                    await asyncio.sleep(30)
            except asyncio.CancelledError:
                logger.debug("Welrok device %s run task cancelled", self._id)
                break  # Exit on cancellation
            except Exception:
                logger.exception("Error in device %s run, will retry in 10 seconds", self._id)
                if self._mqtt is not None:
                    try:
                        self._mqtt.stop()
                    except Exception:
                        logger.exception("Error stopping MQTT for device %s", self._id)
                await asyncio.sleep(10)  # Delay before retry
        # Final cleanup
        try:
            self.unsubscribe_all()
        except Exception:
            logger.exception("Exception when unsubscribing for device %s", self._id)

        if self._mqtt is not None:
            try:
                self._mqtt.stop()
            except Exception:
                logger.exception("Error while stopping mqtt client for device %s", self._id)

        # Close aiohttp session
        await self._close_session()

    def get_load(self, telemetry):
        load = "off"
        if "f.0" in telemetry:
            load = config.PARAMS_CHOISE["load"](telemetry["f.0"])
        return load

    def mqtt_data_callback(self, _, __, msg):
        if self._wb_mqtt_device is None:
            logger.warning("MQTT callback received but wb_mqtt_device is None for %s", self._id)
            return

        try:
            topic_name = [
                config.INNER_TOPICS.get(i) for i in msg.topic.split("/") if config.INNER_TOPICS.get(i)
            ]
            if len(topic_name) > 0:
                msg = msg.payload.decode("utf-8")
                if topic_name[0] == "Power":
                    msg = "0" if msg == "1" else "1"
                elif topic_name[0] == "Load":
                    msg = "on" if msg == "1" else "off"
                elif "temperature" in topic_name[0]:
                    if "open" not in msg and "Set " not in topic_name[0]:
                        if topic_name[0] != "Floor temperature":
                            msg = str(round(float(msg)))
                        msg = f"{msg} \u00B0C"
                elif topic_name[0] == "Current mode":
                    msg = config.MODE_NAMES_TRANSLATE[msg]
                self._wb_mqtt_device.update(topic_name[0], msg)
        except Exception:
            logger.exception("Error in mqtt_data_callback for device %s", self._id)

    async def send_command_http(self, data):
        try:
            session = self._get_session()
            async with session.post(self._url, data=json.dumps(data)) as response:
                response = await response.read()
                logger.debug(
                    "Welrok device %s send_command_http response = %s",
                    self._id,
                    json.loads(response.decode()),
                )
                return json.loads(response.decode())
        except asyncio.TimeoutError:
            logger.error("send_command_http timeout after %s seconds", HTTP_REREQUEST_TIMEOUT)
            return None

    async def set_power(self, power: int):
        if self._mqtt_enable:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self._mqtt.publish, self._mqtt_pub_base_topic + config.PARAMS_CODES[125], str(power)
                    ),
                    timeout=MQTT_PUBLISH_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning("MQTT publish timeout for device %s", self._id)
        else:
            command = {"sn": self._sn, "par": [[125, 7, str(power)]]}
            logger.debug("Welrok device %s send_command_http = %s", self._id, command)
            await self.send_command_http(command)

    async def set_temp(self, temp: int):
        if 5 <= temp <= 45:
            if self._mqtt_enable:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._mqtt.publish, self._mqtt_pub_base_topic + config.PARAMS_CODES[31], str(temp)
                        ),
                        timeout=MQTT_PUBLISH_TIMEOUT,
                    )
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._mqtt.publish,
                            self._mqtt_pub_base_topic + config.PARAMS_CODES[2],
                            str(config.MODE_CODES_REVERSE["Manual"]),
                        ),
                        timeout=MQTT_PUBLISH_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logger.warning("MQTT publish timeout for device %s", self._id)
            else:
                logger.debug("Welrok device %s send_command_http send_temp = %s", self._id, temp)
                await self.send_command_http(
                    {
                        "sn": self._sn,
                        "par": [[2, 2, str(config.MODE_CODES_REVERSE["Manual"])], [31, 3, str(temp * 10)]],
                    }
                )

    async def set_mode(self, new_mode: str):
        try:
            mode_list = [
                config.MODE_CODES_REVERSE.get(i)
                for i in new_mode.split("/")
                if config.MODE_CODES_REVERSE.get(i) is not None
            ]
            if not mode_list:
                logger.debug("No mode mapping for %s", new_mode)
                return
            mode = mode_list[0]
            if self._mqtt_enable:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._mqtt.publish, self._mqtt_pub_base_topic + config.PARAMS_CODES[2], str(mode)
                        ),
                        timeout=MQTT_PUBLISH_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logger.warning("MQTT publish timeout for device %s", self._id)
            else:
                await self.send_command_http({"sn": self._sn, "par": [[2, 2, str(mode)]]})
        except Exception:
            logger.exception("Error in set_mode for device %s", self._id)

    async def set_bright(self, bright: int):
        if 0 <= bright <= 10:
            if self._mqtt_enable:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._mqtt.publish,
                            self._mqtt_pub_base_topic + config.PARAMS_CODES[23],
                            str(bright),
                        ),
                        timeout=MQTT_PUBLISH_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logger.warning("MQTT publish timeout for device %s", self._id)
            else:
                await self.send_command_http({"sn": self._sn, "par": [[23, 2, str(bright)]]})

    def unsubscribe_all(self):
        if not hasattr(self, "_subscribed_topics"):
            return
        if self._mqtt is None:
            self._subscribed_topics.clear()
            return

        for topic in list(self._subscribed_topics):
            try:
                try:
                    self._mqtt.message_callback_remove(topic)
                except Exception:
                    logger.exception("Failed to remove message callback for %s on device %s", topic, self._id)

                try:
                    self._mqtt.unsubscribe(topic)
                except Exception:
                    logger.exception("Failed to unsubscribe %s for device %s", topic, self._id)
            finally:
                if topic in self._subscribed_topics:
                    self._subscribed_topics.remove(topic)

        self._subscribed_topics.clear()


class WelrokClient:
    def __init__(self, devices_config):
        self.mqtt_client_running = False
        self.devices_config = devices_config
        self.mqtt_server_uri = (
            devices_config[0]["mqtt_server_uri"] if devices_config and len(devices_config) > 0 else None
        )

    async def _exit_gracefully(self):
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _on_mqtt_client_connect(self, _, __, ___, rc):
        if rc == 0:
            self.mqtt_client_running = True
            logger.info("MQTT client connected")

    def _on_mqtt_client_disconnect(self, _, userdata, rc):
        self.mqtt_client_running = False
        # Normal disconnect (rc == 0) - log and keep service running.
        if rc == 0:
            logger.info("MQTT client disconnected normally (rc=%s)", rc)
            return

        # Error disconnect (rc != 0) - log and schedule graceful shutdown so
        # supervisor/systemd can handle restarts or repairs if needed.
        logger.warning("MQTT client disconnected with error (rc=%s), scheduling shutdown", rc)
        if userdata is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._exit_gracefully(), userdata)
            except Exception:
                logger.exception("Error scheduling exit on disconnect")

    def _on_term_signal(self):
        asyncio.create_task(self._exit_gracefully())
        logger.info("SIGTERM or SIGINT received, exiting")

    async def _wait_for_mqtt_connect(self):
        while not self.mqtt_client_running:
            await asyncio.sleep(0.1)

    async def run(self):
        mqtt_devices = []
        active_devices = {}
        initializing = set()
        monitor_interval = 5

        async def init_device(device_config):
            device_id = device_config.get("device_id")
            if not device_id:
                logger.error("Device config has no device_id: %s", device_config)
                return
            if device_id in initializing:
                return
            initializing.add(device_id)
            try:
                entry = active_devices.get(device_id)
                if entry and not entry["task"].done():
                    return

                welrok_device = WelrokDevice(device_config)
                logger.debug("Initializing Welrok_device: %s", welrok_device)

                if not welrok_device._url:
                    logger.debug("Device %s has no HTTP URL, creating MQTT-only device", welrok_device.id)
                    default_state = {
                        "powerOff": 1,
                        "bright": 9,
                        "setTemp": 20,
                        "mode": "0",
                        "read_only_temp": {},
                        "load": "off",
                    }
                    mqtt_device = MQTTDevice(mqtt_client, default_state)

                    prev = active_devices.pop(device_id, None)
                    if prev and prev.get("mqtt"):
                        try:
                            prev["mqtt"].remove()
                        except Exception:
                            logger.exception("Error removing previous mqtt device for %s", device_id)

                    mqtt_devices.append(mqtt_device)
                    mqtt_device.set_welrok_device(welrok_device)
                    welrok_device.set_mqtt_device(mqtt_device)
                    mqtt_device.publicate()
                    task = asyncio.create_task(welrok_device.run())
                    active_devices[device_id] = {"task": task, "welrok": welrok_device, "mqtt": mqtt_device}

                    def _done_callback(t, dev_id=device_id):
                        logger.info("Device task %s finished", dev_id)
                        entry2 = active_devices.pop(dev_id, None)
                        if entry2 and entry2.get("mqtt"):
                            try:
                                entry2["mqtt"].remove()
                            except Exception:
                                logger.exception("Error removing mqtt device %s", dev_id)

                    task.add_done_callback(_done_callback)
                    return

                params_states = await welrok_device.get_device_state(config.CMD_CODES["params"])
                if params_states is None:
                    logger.error(
                        "Device %s params unavailable, skipping device initialization", welrok_device.id
                    )
                    return

                device_controls_state = welrok_device.parse_device_params_state(params_states) or {}
                telemetry = await welrok_device.get_device_state(config.CMD_CODES["telemetry"]) or {}
                device_controls_state.update(
                    {
                        "read_only_temp": welrok_device.parse_temperature_response(telemetry),
                        "load": welrok_device.get_load(telemetry),
                    }
                )

                mqtt_device = MQTTDevice(mqtt_client, device_controls_state)

                prev = active_devices.pop(device_id, None)
                if prev and prev.get("mqtt"):
                    try:
                        prev["mqtt"].remove()
                    except Exception:
                        logger.exception("Error removing previous mqtt device for %s", device_id)

                mqtt_devices.append(mqtt_device)
                mqtt_device.set_welrok_device(welrok_device)
                welrok_device.set_mqtt_device(mqtt_device)
                mqtt_device.publicate()
                task = asyncio.create_task(welrok_device.run())
                active_devices[device_id] = {"task": task, "welrok": welrok_device, "mqtt": mqtt_device}

                def _done_callback(t, dev_id=device_id):
                    logger.info("Device task %s finished", dev_id)
                    entry2 = active_devices.pop(dev_id, None)
                    if entry2 and entry2.get("mqtt"):
                        try:
                            entry2["mqtt"].remove()
                        except Exception:
                            logger.exception("Error removing mqtt device %s", dev_id)

                task.add_done_callback(_done_callback)

            except Exception:
                logger.exception(
                    "Failed to initialize device %s — skipping", device_config.get("device_title")
                )
            finally:
                initializing.discard(device_id)

        async def monitor_devices():
            while True:
                try:
                    configured = list(self.devices_config)  # snapshot
                    configured_ids = set()

                    for device_config in configured:
                        device_id = device_config.get("device_id")
                        if not device_id:
                            logger.warning("Skipping device with no id in config: %s", device_config)
                            continue
                        configured_ids.add(device_id)
                        entry = active_devices.get(device_id)
                        if not entry or entry["task"].done():
                            asyncio.create_task(init_device(device_config))

                    for dev_id in list(active_devices.keys()):
                        if dev_id not in configured_ids:
                            entry = active_devices.pop(dev_id, None)
                            if entry:
                                logger.info(
                                    "Device %s removed from config — cancelling task and cleaning up", dev_id
                                )
                                try:
                                    entry["task"].cancel()
                                except Exception:
                                    logger.exception("Error cancelling device task %s", dev_id)
                                try:
                                    if entry.get("mqtt"):
                                        entry["mqtt"].remove()
                                except Exception:
                                    logger.exception("Error removing mqtt device %s", dev_id)

                    await asyncio.sleep(monitor_interval)
                except asyncio.CancelledError:
                    logger.debug("monitor_devices cancelled")
                    break
                except Exception:
                    logger.exception("Error in monitor_devices loop")

        try:
            event_loop = asyncio.get_event_loop()
            event_loop.add_signal_handler(signal.SIGTERM, self._on_term_signal)
            event_loop.add_signal_handler(signal.SIGINT, self._on_term_signal)
            mqtt_client = MQTTClient("welrok", self.mqtt_server_uri or DEFAULT_BROKER_URL)
            mqtt_client.user_data_set(event_loop)
            mqtt_client.on_connect = self._on_mqtt_client_connect
            mqtt_client.on_disconnect = self._on_mqtt_client_disconnect
            mqtt_client.start()
            try:
                await asyncio.wait_for(self._wait_for_mqtt_connect(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "MQTT client did not connect within timeout; publications may be queued or lost"
                )

            logger.debug("MQTT client started")

            monitor_task = asyncio.create_task(monitor_devices())

            await monitor_task

        except (ConnectionError, ConnectionRefusedError) as e:
            logger.error("MQTT error connection to broker %s: %s", DEFAULT_BROKER_URL, e)
            return 1
        except asyncio.CancelledError:
            logger.debug("Run welrok client task cancelled")
            try:
                monitor_task.cancel()
            except Exception:
                pass
            for dev_id, entry in list(active_devices.items()):
                try:
                    entry["task"].cancel()
                except Exception:
                    logger.exception("Error cancelling device %s", dev_id)
                try:
                    if entry.get("mqtt"):
                        entry["mqtt"].remove()
                except Exception:
                    logger.exception("Error removing mqtt device %s", dev_id)
            return 0 if self.mqtt_client_running else 1
        except Exception as e:
            logger.debug(f"Error: {e}.\n{traceback.format_exc()}")
        finally:
            try:
                if "monitor_task" in locals():
                    monitor_task.cancel()
            except Exception:
                pass

            for dev_id, entry in list(active_devices.items()):
                try:
                    entry["task"].cancel()
                except Exception:
                    logger.exception("Error cancelling device %s", dev_id)
                if entry.get("mqtt"):
                    try:
                        entry["mqtt"].remove()
                    except Exception:
                        logger.exception("Error removing mqtt device %s", dev_id)

            if self.mqtt_client_running:
                for mqtt_device in mqtt_devices:
                    try:
                        mqtt_device.remove()
                    except Exception:
                        logger.exception("Error removing mqtt_device")
                mqtt_client.stop()
                logger.debug("MQTT client stopped")


def read_and_validate_config(config_filepath: str, schema_filepath: str) -> dict:
    with open(config_filepath, "r", encoding="utf-8") as config_file, open(
        schema_filepath, "r", encoding="utf-8"
    ) as schema_file:
        try:
            config = json.load(config_file)
            schema = json.load(schema_file)
            jsonschema.validate(config, schema)

            id_list = [device["device_id"] for device in config["devices"]]
            if len(id_list) != len(set(id_list)):
                raise ValueError("Device ID must be unique")

            return config
        except (
            jsonschema.exceptions.ValidationError,
            ValueError,
            DeprecationWarning,
        ) as e:
            logger.error("Config file validation failed! Error: %s", e)
            return None


def to_json(config_filepath: str) -> dict:
    with open(config_filepath, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
        return config


def main(argv=sys.argv):
    logger.info("Welrok service starting")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        action="store_true",
        help=f"Make JSON for wb-mqtt-confed from {config.CONFIG_FILEPATH}",
    )
    parser.add_argument("-c", "--config", type=str, default=config.CONFIG_FILEPATH, help="Config file")
    args = parser.parse_args(argv[1:])

    if args.j:
        config_file = to_json(args.config)
        json.dump(config_file, sys.stdout, sort_keys=True, indent=2)
        return 0

    config_file = read_and_validate_config(args.config, config.SCHEMA_FILEPATH)
    if config_file is None:
        return 6
    if config_file["debug"]:
        logging.basicConfig(level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    devices = config_file["devices"]

    welrok_client = WelrokClient(devices)
    result = asyncio.run(welrok_client.run())
    logger.info("Welrok service stopped")
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
