import asyncio
import logging
import time
import traceback
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple

import aiohttp

from wb_welrok import config
from wb_welrok.decorators import retry
from wb_welrok.mqtt_client import MQTTClient
from wb_welrok.schemas import DeviceParam
from wb_welrok.wb_mqtt_device import MQTTDevice

if TYPE_CHECKING:
    from wb_welrok.schemas import DeviceConfig


logger = logging.getLogger(__name__)


class MsgProcessor:

    def __init__(self, temp_formater: Callable):
        self.config = config
        self.temp_formater = temp_formater

    def Power(self, msg) -> str:
        return "0" if msg == "1" else "1"

    def Load(self, msg) -> str:
        return "Включено" if msg == "1" else "Выключено"

    def temperature(self, msg, topic) -> Optional[str]:
        if "open" not in msg and "Set " not in topic:
            return self.temp_formater(msg)

    def Current_mode(self, msg) -> Optional[str]:
        return self.config.MODE_NAMES_TRANSLATE.get(msg, msg)

    def process(self, topic, msg):
        method_name = topic.replace(" ", "_")
        if "temperature" in topic:
            return self.temperature(msg, topic)
        method = getattr(self, method_name, None)
        if method:
            return method(msg)
        return None


class WelrokDataParser:

    def __init__(self):
        self.msg_processor = MsgProcessor(self.temp_formater)
        self._temp_div = config.DefaultParseValue.TEMP_DIV.value
        self._temp_data_type = config.HttpCode.TEMP.value
        self.upper_limit_temp = config.DefaultParseValue.UPPER_LIMIT_TEMP.value
        self.lower_limit_temp = config.DefaultParseValue.LOWER_LIMIT_TEMP.value
        self.upper_limit_air_temp = config.DefaultParseValue.UPPER_LIMIT_TEMP.value
        self.lower_limit_air_temp = config.DefaultParseValue.LOWER_LIMIT_TEMP.value
        self.upper_limit_bright = config.DefaultParseValue.UPPER_LIMIT_BRIGHT.value
        self.lower_limit_bright = config.DefaultParseValue.LOWER_LIMIT_BRIGHT.value

    def temp_formater(self, temp):
        try:
            return f"{round(float(temp), 2)} \u00b0C"
        except (ValueError, TypeError):
            return str(temp)

    def parse_power_off(self, par: DeviceParam):
        return "1" if par.value == "0" else "0"

    def parse_bright(self, par: DeviceParam):
        return round(float(par.value), 2)

    def parse_set_temp(self, par: DeviceParam):
        self._temp_div = 10 if par.data_type == 3 else 1
        self._temp_data_type = par.data_type
        return round(float(par.value) / self._temp_div, 2)

    def parse_limit(self, par: DeviceParam):
        temp_div = 10 if par.data_type == 3 else 1
        scaled = int(round(float(par.value) / temp_div, 0))
        if par.code == config.ParamCode.UPPER_LIMIT.value:
            self._upper_limit = par.value
            self.upper_limit_temp = scaled
        if par.code == config.ParamCode.LOWER_LIMIT.value:
            self._lower_limit = par.value
            self.lower_limit_temp = scaled
        return None

    def parse_control_type(self, par: DeviceParam):
        return str(int(par.value))

    def parse_air_limit(self, par: DeviceParam):
        scaled = int(round(float(par.value), 0))
        if par.code == config.ParamCode.UPPER_AIR_LIMIT.value:
            self.upper_limit_air_temp = scaled
        elif par.code == config.ParamCode.LOWER_AIR_LIMIT.value:
            self.lower_limit_air_temp = scaled
        return None

    def parse_mode(self, par: DeviceParam):
        return config.MODE_CODES.get(int(par.value), "unknown")

    def params_parsers(self) -> Dict[int, Tuple[Callable[[DeviceParam], Any], str]]:
        return {
            config.ParamCode.POWER.value: (self.parse_power_off, "powerOff"),
            config.ParamCode.BRIGHT.value: (self.parse_bright, "bright"),
            config.ParamCode.TEMP.value: (self.parse_set_temp, "setTemp"),
            config.ParamCode.MODE.value: (self.parse_mode, "mode"),
            config.ParamCode.CONTROL_TYPE.value: (self.parse_control_type, "controlType"),
            config.ParamCode.UPPER_LIMIT.value: (self.parse_limit, ""),
            config.ParamCode.LOWER_LIMIT.value: (self.parse_limit, ""),
            config.ParamCode.UPPER_AIR_LIMIT.value: (self.parse_air_limit, ""),
            config.ParamCode.LOWER_AIR_LIMIT.value: (self.parse_air_limit, ""),
        }

    def get_inner_topic(self, key):
        return config.INNER_TOPICS.get(key)

    def mqtt_msg_parse(self, mqtt_msg):
        topic_name = self.get_inner_topic(mqtt_msg.topic.split("/")[-1])
        if topic_name:
            mqtt_msg = mqtt_msg.payload.decode("utf-8")
            return topic_name, self.msg_processor.process(topic_name, mqtt_msg)
        return None, None

    def parse_load(self, telemetry):
        try:
            load_val = telemetry.get(config.StateCode.LOAD.value, "0")
            return config.PARAMS_CHOISE["load"](load_val)
        except Exception as e:
            logger.exception("Error getting load from telemetry: %s", e)
            return "off"

    def parse_temperature_response(self, data: dict) -> dict:
        current_temp = {}
        try:
            for code in config.TemperatureCode:
                if code.key in data:
                    val = int(data[code.key])
                    temp = round(val / code.divisor, code.precision)
                    current_temp[code.title] = str(temp)
            for fault_code in config.FaultCode:
                if fault_code == config.FaultCode.AIR_SENSOR_BATTERY:
                    continue
                if data.get(fault_code.code) == "1":
                    current_temp[fault_code.control_title] = fault_code.error_text
        except Exception as e:
            logger.exception("Error parsing temperature response: %s", e)
        return current_temp

    def parse_sensor_errors(self, data: dict) -> dict:
        errors = {}
        for fault_code in config.FaultCode:
            title = fault_code.control_title
            if data.get(fault_code.code) == "1":
                if not errors.get(title):
                    errors[title] = fault_code.error_text
            elif title not in errors:
                errors[title] = ""
        return errors

    def parse_device_params_state(self, data: dict) -> dict:
        state = {}
        params = {param.code: param for par in data.get("par", []) for param in [DeviceParam(*par)]}
        try:
            for code, (parser_func, param_name) in self.params_parsers().items():
                param = params.get(code)
                if param:
                    data_to_state = parser_func(param)
                    if data_to_state:
                        state[param_name] = data_to_state
            logger.debug("Parsed device params state: %s", state)
        except Exception as e:
            logger.exception("Error parsing device params state: %s", e)
        return state

    def format_command(self, param: config.ParamCode, value, http_code: Optional[config.HttpCode] = None):
        mqtt_data = [(config.PARAMS_CODES[param], value)]
        if param == config.ParamCode.TEMP and isinstance(value, (int, float)):
            scaled_value = str(int(value * self._temp_div))
        else:
            scaled_value = str(value)
        http_params = [[param.value, http_code.value, scaled_value]] if http_code else []
        return mqtt_data, http_params

    def format_bright(self, bright):
        if bright > 0:
            return max(1, bright // 10)
        return 0


class WelrokDevice:
    def __init__(self, properties: "DeviceConfig", mqtt_server_uri, root_mqtt):
        self._root_mqtt = root_mqtt
        self._id = properties.device_id
        self._title = properties.device_title
        self._sn = properties.serial_number
        self._ip = properties.device_ip
        self._mqtt_enable = properties.mqtt_enable
        self._mqtt_server_uri = mqtt_server_uri
        self._url = f"http://{properties.device_ip}/api.cgi"
        self._wb_mqtt_device = None
        self._mqtt = MQTTClient(self._title, self._mqtt_server_uri)
        self._mqtt.user_data_set(self)
        self._subscribed_topics = set()
        self._mqtt_pub_base_topic = f"{properties.inner_mqtt_pubprefix}{properties.inner_mqtt_client_id}/set/"
        self._mqtt_sub_base_topic = f"{properties.inner_mqtt_subprefix}{properties.inner_mqtt_client_id}/get/"
        self._mqtt_data_topics = config.data_topics
        self._mqtt_settings_topics = config.settings_topics
        self._data_parser = WelrokDataParser()
        self._bright_check = (
            lambda x: self._data_parser.lower_limit_bright <= x <= self._data_parser.upper_limit_bright
        )
        self._temp_check = lambda x: (
            self._data_parser.lower_limit_air_temp <= x <= self._data_parser.upper_limit_air_temp
            if self._control_type in (1, 2)
            else self._data_parser.lower_limit_temp <= x <= self._data_parser.upper_limit_temp
        )
        self._temp_div_coef = 1
        self._legacy_bright = False
        self._pending_bright_detection = False
        self._current_mode: Optional[str] = None
        self._control_type: Optional[int] = None
        self._pending_set_temp: Optional[int] = None
        self._pending_set_temp_expire: float = 0.0
        self._pending_mode: Optional[str] = None
        self._pending_mode_expire: float = 0.0

        self._params_failures = 0
        self._telemetry_failures = 0
        self.__session: Optional[aiohttp.ClientSession] = None

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

    async def close_session(self):
        if self.__session and not self.__session.closed:
            await self.__session.close()

    @property
    def _session(self) -> aiohttp.ClientSession:
        if self.__session is None or self.__session.closed:
            timeout = aiohttp.ClientTimeout(total=config.HTTP_REREQUEST_TIMEOUT)
            connector = aiohttp.TCPConnector(
                limit_per_host=2,
                force_close=True,
                enable_cleanup_closed=True,
                ssl=False,
            )
            self.__session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)
        return self.__session

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

    def set_mqtt_device(self, device_controls_state, telemetry):
        device_controls_state.update(
            {
                "read_only_temp": self._data_parser.parse_temperature_response(telemetry),
                "load": self.get_load(telemetry),
            }
        )
        self._wb_mqtt_device = MQTTDevice(self._root_mqtt, device_controls_state, self)
        logger.debug("Set WB MQTT device for Welrok %s", self._id)

    @retry(
        retries=config.HTTP_PERIODIC_RETRIES,
        delay=0.5,
        exceptions=(asyncio.TimeoutError, aiohttp.ClientError),
    )
    async def get_device_state(self, cmd: int) -> Optional[dict]:
        async with self._session.post(self._url, json={"cmd": cmd}) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.error("HTTP error %s for device %s", resp.status, self._id)
                return None

    async def set_current_temp(self, current_temp: dict):
        for key, value in current_temp.items():
            display_value = self._data_parser.temp_formater(value)
            logger.debug("Welrok device %s setting readonly temp %s = %s", self._id, key, display_value)
            if self._wb_mqtt_device:
                self._wb_mqtt_device.set_readonly(key, display_value)

    def _update_sensor_error_flags(self, telemetry: dict):
        if not self._wb_mqtt_device:
            return
        sensor_errors = self._data_parser.parse_sensor_errors(telemetry)
        for control_title, error_text in sensor_errors.items():
            if self._wb_mqtt_device.has_control(control_title):
                self._wb_mqtt_device.set_control_error_state(control_title, error_text)

    async def set_current_control_state(self, current_states: dict):
        for key, value in current_states.items():
            logger.debug("Welrok device %s updating control %s with value %s", self.id, key, value)
            if self._wb_mqtt_device:
                self._wb_mqtt_device.update(key, str(value))

    def _on_mqtt_connect(self, client, _, __, rc):
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

    def control_states(self, device_controls_state):
        bright_raw = int(device_controls_state.get("bright", 0))
        if self._legacy_bright and bright_raw == 9:
            bright_ui = 100
        else:
            bright_ui = bright_raw * 10

        set_temp = int(device_controls_state.get("setTemp", 0))
        if self._pending_set_temp is not None:
            if time.monotonic() > self._pending_set_temp_expire:
                self._pending_set_temp = None
            elif set_temp == self._pending_set_temp:
                self._pending_set_temp = None
            else:
                set_temp = self._pending_set_temp

        return {
            "Power": int(device_controls_state.get("powerOff", 0)),
            "Bright": bright_ui,
            "Set temperature": set_temp,
        }

    async def set_params(self, device_controls_state, telemetry):
        if self._pending_bright_detection:
            self._pending_bright_detection = False
            bright_from_device = int(device_controls_state.get("bright", 0))
            if bright_from_device == 9:
                self._legacy_bright = True
                self._data_parser.upper_limit_bright = 9
                logger.info("Legacy brightness detected for device %s (max bright = 9)", self._id)
        self._current_mode = device_controls_state.get("mode")
        control_type_str = device_controls_state.get("controlType")
        if control_type_str is not None:
            self._control_type = int(control_type_str)
        await self.set_current_control_state(self.control_states(device_controls_state))
        if self._wb_mqtt_device:
            if self._control_type in (1, 2):
                self._wb_mqtt_device.update_temp_limits(
                    int(self._data_parser.lower_limit_air_temp),
                    int(self._data_parser.upper_limit_air_temp),
                )
            else:
                self._wb_mqtt_device.update_temp_limits(
                    int(self._data_parser.lower_limit_temp),
                    int(self._data_parser.upper_limit_temp),
                )
        await self.set_current_temp(self._data_parser.parse_temperature_response(telemetry))
        self._update_sensor_error_flags(telemetry)
        if self._wb_mqtt_device:
            device_mode = device_controls_state.get("mode")
            if self._pending_mode is not None:
                if time.monotonic() > self._pending_mode_expire:
                    self._pending_mode = None
                elif device_mode == self._pending_mode:
                    self._pending_mode = None
            display_mode = self._pending_mode if self._pending_mode is not None else device_mode
            self._wb_mqtt_device.set_readonly(
                "Current mode",
                config.MODE_NAMES_TRANSLATE.get(display_mode or "", ""),
            )
            self._wb_mqtt_device.set_readonly("Load", self.get_load(telemetry))

    async def run(self):
        try:
            self._mqtt.on_connect = self._on_mqtt_connect
            self._mqtt.on_disconnect = self._on_mqtt_disconnect
            self._mqtt.start()
            while True:
                try:
                    telemetry = await self.get_device_state(config.CmdCode.TELEMETRY.value)
                    params_response = await self.get_device_state(config.CmdCode.PARAMS.value)
                    if params_response is not None and telemetry is not None:
                        self._params_failures = 0
                        self._telemetry_failures = 0
                        device_controls_state = (
                            self._data_parser.parse_device_params_state(params_response) or {}
                        )
                        if self._wb_mqtt_device:
                            await self.set_params(device_controls_state, telemetry)
                        else:
                            self.set_mqtt_device(device_controls_state, telemetry)
                    else:
                        self._params_failures = min(self._params_failures + 1, config.HTTP_FAILURE_THRESHOLD)
                        if self._params_failures == config.HTTP_FAILURE_THRESHOLD:
                            logger.warning(
                                "Device %s params unavailable (failed %s times)",
                                self._id,
                                self._params_failures,
                            )
                        else:
                            logger.debug(
                                "Device %s transient params error (%s/%s)",
                                self._id,
                                self._params_failures,
                                config.HTTP_FAILURE_THRESHOLD,
                            )
                        self._telemetry_failures = min(
                            self._telemetry_failures + 1, config.HTTP_FAILURE_THRESHOLD
                        )
                        if self._telemetry_failures == config.HTTP_FAILURE_THRESHOLD:
                            logger.warning(
                                "Device %s telemetry unavailable (failed %s times)",
                                self._id,
                                self._telemetry_failures,
                            )
                        else:
                            logger.debug(
                                "Device %s transient telemetry error (%s/%s)",
                                self._id,
                                self._telemetry_failures,
                                config.HTTP_FAILURE_THRESHOLD,
                            )

                    await asyncio.sleep(10)

                except asyncio.CancelledError:
                    logger.info("Welrok device %s run task cancelled", self._id)
                    break

                except (aiohttp.ClientConnectorError, OSError) as e:
                    logger.warning("Device %s connection error, restarting after delay: %s", self._id, e)
                    try:
                        self._mqtt.stop()
                    except Exception:
                        logger.exception("Error stopping MQTT for device %s", self._id)
                    await asyncio.sleep(10)

                except Exception:
                    logger.exception("Error in device %s run loop, restarting after delay", self._id)
                    try:
                        self._mqtt.stop()
                    except Exception:
                        logger.exception("Error stopping MQTT for device %s", self._id)
                    await asyncio.sleep(10)

        finally:
            try:
                self.unsubscribe_all()
            except Exception:
                logger.exception("Exception when unsubscribing for device %s", self._id)

            if self._mqtt is not None:
                try:
                    self._mqtt.stop()
                except Exception:
                    logger.exception("Error while stopping mqtt client for device %s", self._id)

    def _on_mqtt_disconnect(self, _, __, rc):
        if rc != 0:
            logger.warning("Unexpected MQTT disconnect for device %s, rc=%s", self._id, rc)
        else:
            logger.info("MQTT disconnected normally for device %s", self._id)

    def get_load(self, telemetry: dict) -> str:
        return self._data_parser.parse_load(telemetry)

    def mqtt_data_callback(self, _, __, msg):
        if self._wb_mqtt_device is None:
            logger.warning("MQTT callback received but wb_mqtt_device is None for %s", self._id)
            return
        try:
            topic_name, msg = self._data_parser.mqtt_msg_parse(msg)
            if topic_name and msg:
                self._wb_mqtt_device.update(topic_name, msg)
        except Exception:
            logger.exception("Error in mqtt_data_callback for device %s", self._id)

    async def send_command_http(self, data: dict):
        logger.debug("send_command_http для %s , data: %s", self._id, data)
        if not self._url:
            logger.error("HTTP URL не задан для устройства %s", self._id)
            return None
        try:
            async with self._session.post(self._url, json=data) as response:
                if response.status == 200:
                    resp_json = await response.json()
                    logger.debug("HTTP команда для %s выполнена успешно: %s", self._id, resp_json)
                    return resp_json
                else:
                    logger.error("HTTP ошибка %s при отправке команды для %s", response.status, self._id)
                    return None
        except asyncio.TimeoutError:
            logger.error(
                "HTTP запрос таймаут для устройства %s после %s секунд",
                self._id,
                config.HTTP_REQUEST_TIMEOUT,
            )
        except aiohttp.ClientError as e:
            logger.error("HTTP ошибка клиента для устройства %s: %s", self._id, e)
        except Exception:
            logger.exception("Неожиданная ошибка при отправке HTTP команды для %s", self._id)

    async def send_command_mqtt(self, topic, value):
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._mqtt.publish, topic, str(value)),
                timeout=config.MQTT_PUBLISH_TIMEOUT,
            )
            logger.info("%s set to %s on device %s via MQTT", topic, value, self._id)
        except asyncio.TimeoutError:
            logger.warning("MQTT publish timeout on set_power for device %s", self._id)
        except Exception:
            logger.exception("Error publishing set_power for device %s", self._id)

    async def send_command(self, mqtt_data=None, http_params=None):
        try:
            if self._mqtt_enable and mqtt_data:
                for topic_suffix, value in mqtt_data:
                    topic = self._mqtt_pub_base_topic + topic_suffix
                    await self.send_command_mqtt(topic, value)
                    await asyncio.sleep(1)
            if http_params is not None:
                command = {"sn": self._sn, "par": http_params}
                await self.send_command_http(command)
                await asyncio.sleep(1)
            if mqtt_data is None and http_params is None:
                logger.error("Invalid command parameters for device %s", self._id)
        except Exception:
            logger.exception("Error sending command for device %s", self._id)

    async def set_power(self, power: int):
        mqtt_data, http_params = self._data_parser.format_command(
            config.ParamCode.POWER, power, config.HttpCode.POWER
        )
        await self.send_command(mqtt_data, http_params)

    async def set_temp(self, temp: int):
        if not self._temp_check(temp):
            logger.warning("Temperature %s out of range for device %s", temp, self._id)
            return

        self._pending_set_temp = temp
        self._pending_set_temp_expire = time.monotonic() + 30

        need_mode_switch = self._current_mode != "Manual" and not (
            config.SET_TEMP_KEEP_SCHEDULE_MODE and self._current_mode == "Auto"
        )
        if need_mode_switch:
            mqtt_data_mode, http_params_mode = self._data_parser.format_command(
                config.ParamCode.MODE, str(config.ModeCode.MANUAL.value), config.HttpCode.MODE
            )
            await self.send_command(mqtt_data=mqtt_data_mode, http_params=http_params_mode)

        scaled_value = str(int(temp * self._data_parser._temp_div))
        temp_data_type = self._data_parser._temp_data_type
        mqtt_data_temp = [("setTemp", temp)]
        in_schedule_mode = config.SET_TEMP_KEEP_SCHEDULE_MODE and self._current_mode == "Auto"
        if in_schedule_mode:
            http_params_temp = [[config.ParamCode.TEMP_SCHEDULE.value, temp_data_type, scaled_value]]
        elif self._control_type in (1, 2):
            http_params_temp = [[config.ParamCode.MANUAL_AIR_TEMP.value, temp_data_type, scaled_value]]
        else:
            http_params_temp = [[config.ParamCode.MANUAL_FLOOR_TEMP.value, temp_data_type, scaled_value]]
        await self.send_command(mqtt_data=mqtt_data_temp, http_params=http_params_temp)

    async def set_mode(self, new_mode: str, topic):
        if "Manual/on" in topic:
            new_mode = "1"
            pending = "Manual"
        elif "Auto/on" in topic:
            new_mode = "0"
            pending = "Auto"
        else:
            pending = None

        if pending is not None:
            self._pending_mode = pending
            self._pending_mode_expire = time.monotonic() + 30.0

        mqtt_data, http_params = self._data_parser.format_command(
            config.ParamCode.MODE, new_mode, config.HttpCode.MODE
        )
        await self.send_command(mqtt_data=mqtt_data, http_params=http_params)

    async def set_bright(self, bright: int):
        if self._legacy_bright and bright > 9:
            bright = 9
        if not self._bright_check(bright):
            logger.warning("Brightness %s out of range for device %s", bright, self._id)
            return
        if bright == 10:
            self._pending_bright_detection = True
        mqtt_data, http_params = self._data_parser.format_command(
            config.ParamCode.BRIGHT, bright, config.HttpCode.BRIGHT
        )
        await self.send_command(mqtt_data, http_params)

    def unsubscribe_all(self):
        if not hasattr(self, "_subscribed_topics") or self._mqtt is None:
            return
        for topic in list(self._subscribed_topics):
            try:
                self._mqtt.message_callback_remove(topic)
            except Exception:
                logger.exception("Failed to remove message callback for %s on device %s", topic, self._id)
            try:
                self._mqtt.unsubscribe(topic)
            except Exception:
                logger.exception("Failed to unsubscribe %s for device %s", topic, self._id)
            self._subscribed_topics.discard(topic)
