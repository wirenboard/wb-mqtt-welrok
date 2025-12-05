import asyncio
import json
import logging
import traceback

import aiohttp

from wb_welrok import config
from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient
from wb_welrok.wb_mqtt_device import MQTTDevice

MQTT_PUBLISH_TIMEOUT = 5  # seconds
HTTP_REREQUEST_TIMEOUT = 20  # seconds
# How many consecutive failures before we escalate logging to WARNING / mark degraded
HTTP_FAILURE_THRESHOLD = 3
# How many attempts to try when doing the initial device discovery (startup)
HTTP_INIT_RETRIES = 3
# How many attempts to try during periodic polling loop (keep small to avoid long blocking)
HTTP_PERIODIC_RETRIES = 1


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s (%(filename)s:%(lineno)d)")
logger.setLevel(logging.INFO)


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
        """Create a new aiohttp session with timeout and connection limits"""
        timeout = aiohttp.ClientTimeout(total=HTTP_REREQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit_per_host=2,  # Max 2 simultaneous connections per host
            force_close=True,  # Force close connections for unstable devices
            enable_cleanup_closed=True,
            ssl=False,  # Explicitly disable SSL for HTTP connections
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)

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
        """
        Perform POST to device API with retries

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
                async with self._get_session() as session:
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
        pass  # Sessions are now properly closed with async with

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
