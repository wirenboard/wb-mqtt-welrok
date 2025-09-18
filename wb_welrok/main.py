import argparse
import json
import logging
import signal
import sys
import asyncio
import traceback
import aiohttp # type: ignore
import jsonschema # type: ignore

from wb_welrok import config
from wb_welrok import wbmqtt
from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
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
        return (f"MQTTDevice(client={self._client}, device={self._device}, "
                f"welrok_device={self._welrok_device}, root_topic={self._root_topic}, "
                f"device_state={self._device_state})")


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
            "Power", wbmqtt.ControlMeta(title="Включение / выключение", title_en="Power", control_type="switch", order=1, read_only=False),
            self._device_state['powerOff'],
        )
        self._device.add_control_message_callback("Power", self._on_message_power)

        start_bright = str(int(self._device_state['bright']) * 10) if int(self._device_state['bright']) != 9 else '100'
        self._device.create_control(
            "Bright",
            wbmqtt.ControlMeta(title="Яркость дисплея", title_en="Display Brightness", units="%", control_type="range", order=2, read_only=False, max_value=100),
            start_bright,
        )
        self._device.add_control_message_callback("Bright", self._on_message_bright)

        self._device.create_control(
            "Set temperature",
            wbmqtt.ControlMeta(title="Установка", title_en="Set floor temperature", units="deg C", control_type="range", order=3, read_only=False, min_value=5, max_value=45),
            self._device_state['setTemp'],
        )
        self._device.add_control_message_callback("Set temperature", self._on_message_temperature)

        self._device.create_control(
            'Current mode',
            wbmqtt.ControlMeta(title='Текущий режим работы', title_en="Current mode", control_type="text", order=4, read_only=True),
            self._device_state['mode']
        )

        self._device.create_control(
            'Load',
            wbmqtt.ControlMeta(title='Нагрев', title_en="Load", control_type="text", order=5, read_only=True),
            self._device_state['load']
        )

        self._device.create_control(
            "Set temperature value",
            wbmqtt.ControlMeta(title="Установка", title_en="Set floor temperature", control_type="temperature", order=6, read_only=False, min_value=5, max_value=45),
            self._device_state['setTemp'],
        )
        self._device.add_control_message_callback("Set temperature value", self._on_message_temperature_value)

        for order_number, mode_title in enumerate(config.MODE_CODES.values(), 7):
            self._device.create_control(
                mode_title,
                wbmqtt.ControlMeta(title=f'Установить режим работы "{config.MODE_NAMES_TRANSLATE[mode_title]}"', title_en=f'Set mode "{mode_title}"', control_type="pushbutton", order=order_number, read_only=False), "1"
            )
            self._device.add_control_message_callback(mode_title, self._on_message_mode)

        for order_number, read_only_temp in enumerate(self._device_state['read_only_temp'], 7 + len(config.MODE_CODES)):
            self._device.create_control(
                read_only_temp,
                wbmqtt.ControlMeta(title=config.TOPIC_NAMES_TRANSLATE[read_only_temp], title_en=read_only_temp, control_type="text", order=order_number, read_only=True),
                self._device_state['read_only_temp'][read_only_temp],
            )


        logger.info("%s device created", self._root_topic)

    def update(self, control_name, value):
        self._device.set_control_value(control_name, value)
        logger.debug("%s %s control updated with value %s", self._welrok_device.id, control_name, value)

    def set_readonly(self, control_name, value):
        self._device.set_control_read_only(control_name, value)
        self._device.set_control_value(control_name, value)
        logger.debug("%s %s control readonly set to %s", self._welrok_device.id, control_name, value)

    def set_error_state(self, error: bool):
        for control_name in self._device.get_controls_list():
            if control_name != "IP address":
                self._device.set_control_error(control_name, "r" if error else "")

    def remove(self):
        self._device.remove_device()
        logger.info("%s device deleted", self._root_topic)

    def _on_message_power(self, _, __, msg):
        power = 0 if str(msg.payload.decode("utf-8")) == '1' else 1
        asyncio.run_coroutine_threadsafe(self._welrok_device.set_power(power), self._loop).result()
        logger.info("Welrok %s power state changed to %s", self._welrok_device.sn, power)

    def _on_message_temperature(self, _, __, msg):
        temp = int(str(msg.payload.decode("utf-8")))
        asyncio.run_coroutine_threadsafe(self._welrok_device.set_temp(temp), self._loop).result()
        logger.info("Set temperature %s on Welrok %s", temp, self._welrok_device.sn)

    def _on_message_temperature_value(self, _, __, msg):
        temp = int(str(msg.payload.decode("utf-8")))
        asyncio.run_coroutine_threadsafe(self._welrok_device.set_temp(temp), self._loop).result()
        logger.info("Set temperature %s on Welrok %s", temp, self._welrok_device.sn)

    def _on_message_bright(self, _, __, msg):
        bright = int(str(msg.payload.decode("utf-8")))
        if bright > 0:
            if bright < 10:
                bright = 1
            elif bright == 100:
                bright = 9        
            else:
                bright = bright // 10
        asyncio.run_coroutine_threadsafe(self._welrok_device.set_bright(bright), self._loop).result()
        logger.info("Set bright %s on Welrok %s", bright, self._welrok_device.sn)

    def _on_message_mode(self, _, __, msg):
        mode = str(msg.topic)
        asyncio.run_coroutine_threadsafe(self._welrok_device.set_mode(mode), self._loop).result()
        logger.info("Welrok %s mode state changed to %s", self._welrok_device.sn, mode)

class WelrokDevice:
    def __init__(self, properties):
        self._id = properties['device_id']
        self._title = properties['device_title']
        self._sn = properties['serial_number']
        self._ip = properties['device_ip']
        self._mqtt_enable = properties['mqtt_enable']
        self._mqtt_server_uri = properties['mqtt_server_uri'] if properties['mqtt_server_uri'] else None
        self._url = f'http://{properties["device_ip"]}/api.cgi' if properties["device_ip"] else None
        self._wb_mqtt_device = None
        self._mqtt = MQTTClient(properties['device_title'], properties['mqtt_server_uri'] or DEFAULT_BROKER_URL)
        self._mqtt_pub_base_topic = f'{properties["inner_mqtt_pubprefix"]}{properties["inner_mqtt_client_id"]}/set/'
        self._mqtt_sub_base_topic = f'{properties["inner_mqtt_subprefix"]}{properties["inner_mqtt_client_id"]}/get/'
        self._mqtt_data_topics = config.data_topics
        self._mqtt_settings_topics = config.settings_topics

        logger.debug("Add device with id " + self._id + " and sn " + self._sn)
    
    def __repr__(self):
        return (f"WelrokDevice(id={self._id}, title={self._title}, "
                f"serial_number={self._sn}, ip={self._ip}, url={self._url}, "
                f"mqtt_pub_base_topic={self._mqtt_pub_base_topic}, "
                f"mqtt_sub_base_topic={self._mqtt_sub_base_topic}, "
                f"mqtt_data_topics={self._mqtt_data_topics}, "
                f"mqtt_settings_topics={self._mqtt_settings_topics})")

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

    def parse_temperature_response(self, data):
        current_temp = {}
        for code, value in config.TEMP_CODES.items():
            if f't.{code}' in data:
                v = str(round(int(data[f't.{code}']) / 16, 2)) if code == 1 else str(round(int(data[f't.{code}']) / 16))
                current_temp.update({
                    value: v
                })
        for code in range(3,7):
            sensor = 1 if code in (3,4) else 2
            if f'f.{code}' in data and data[f'f.{code}'] == '1':
                current_temp.update({
                    config.TEMP_CODES[sensor]: 'КЗ или обрыв цепи'
                })
        return current_temp
    
    def parse_response(self, raw_response, param):
        for par in raw_response['par']:
            if par[0] == param:
                return par[2]
        return None

    def parse_device_params_state(self, data):
        state = {}
        logger.debug(f'Parse device params state data: {data}')
        try:
            for par in data['par']:
                if par[0] in config.PARAMS_CODES:
                    state.update({
                        config.PARAMS_CODES[par[0]]: config.PARAMS_CHOISE[config.PARAMS_CODES[par[0]]](par[2])
                    })
            logger.debug(f'Parse device params state "STATE": {state}')
            return state
        except Exception as e:
            logger.debug(f'Error: {e}.\n{traceback.format_exc()}')

    async def get_device_state(self, cmd):
        try:
            logger.debug("Start get_device_state")
            post_data = json.dumps({'cmd': cmd})
            logger.debug(f"Get_device_state post_data: {post_data}")
            async with aiohttp.ClientSession() as session:
                async with session.post(self._url, data=post_data) as resp:
                    status = resp.status
                    try:
                        response = await resp.json()
                    except aiohttp.ContentTypeError:
                        logger.debug('Response content is not in JSON format')
                        return None

                    return response
        except Exception as e:
            logger.debug(f'Error: {e}.\n{traceback.format_exc()}')
            return None

    async def set_current_temp(self, current_temp):
        for key, value in current_temp.items():
            if 'open' not in value:
                value = f'{value} \u00B0C'
            logger.debug("Welrok device %s current_temp item key = %s, value = %s", self._id, key, value)
            self._wb_mqtt_device.set_readonly(key, value)

    async def set_current_control_state(self, current_states):
        for key, value in current_states.items():
            logger.debug("Welrok device %s curent state item key = %s, value = %s", self._id, key, value)
            self._wb_mqtt_device.update(key, value)

    async def run(self):
        if self._mqtt is not None:
            self._mqtt.start()
            for topic in self._mqtt_data_topics:
                self._mqtt.subscribe(self._mqtt_sub_base_topic + topic)
                self._mqtt.message_callback_add(self._mqtt_sub_base_topic + topic, self.mqtt_data_callback)
                logger.debug("Welrok device add subscribe inner topic - %s", self._mqtt_sub_base_topic + topic)
        try:
            while True:
                if self._url:
                    device_controls_state = self.parse_device_params_state(await self.get_device_state(config.CMD_CODES['params']))
                    logger.debug(f"device_controls_state: {device_controls_state}")
                    control_states = {
                        "Power": int(device_controls_state['powerOff']),
                        "Bright": int(device_controls_state['bright'] * 10),
                        "Set temperature": int(device_controls_state['setTemp']),
                        "Set temperature value": int(device_controls_state['setTemp'])
                    }
                    self._wb_mqtt_device.set_readonly('Current mode', config.MODE_NAMES_TRANSLATE[device_controls_state['mode']])
                    await self.set_current_control_state(control_states)
                    telemetry = await self.get_device_state(config.CMD_CODES['telemetry'])
                    self._wb_mqtt_device.set_readonly('Load', self.get_load(telemetry))
                    await self.set_current_temp(self.parse_temperature_response(telemetry))
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.debug("Welrok device %s run task cancelled", self._id)
        if self._mqtt is not None:
            for topic in self._mqtt_data_topics:
                self._mqtt.unsubscribe(self._mqtt_sub_base_topic + topic)
                self._mqtt.message_callback_remove(self._mqtt_sub_base_topic + topic)
            self._mqtt.stop()

    def get_load(self, telemetry):
        load = 'off'
        if 'f.0' in telemetry:
            load = config.PARAMS_CHOISE['load'](telemetry['f.0'])
        return load

    def mqtt_data_callback(self, _, __, msg):
        topic_name = [config.INNER_TOPICS.get(i) for i in msg.topic.split('/') if config.INNER_TOPICS.get(i)]
        if len(topic_name) > 0:
            msg = msg.payload.decode('utf-8')
            if topic_name[0] == 'Power':
                msg = '0' if msg == '1' else '1'
            elif topic_name[0] == 'Load':
                msg = 'on' if msg == '1' else 'off'
            elif 'temperature' in topic_name[0]:
                if 'open' not in msg and 'Set ' not in topic_name[0]:
                    if topic_name[0] != 'Floor temperature':
                        msg = str(round(float(msg)))
                    msg = f'{msg} \u00B0C'
            elif topic_name[0] == 'Current mode':
                msg = config.MODE_NAMES_TRANSLATE[msg]
            self._wb_mqtt_device.update(topic_name[0], msg)

    async def send_command_http(self, data):
        async with aiohttp.ClientSession() as session:
            async with session.post(self._url, data=json.dumps(data)) as response:
                response = await response.read()
                logger.debug(f"Welrok device {self._id} send_command_http response = {json.loads(response.decode())}")
                return json.loads(response.decode())

    async def set_power(self, power: int):
        if self._mqtt_enable:
            self._mqtt.publish(self._mqtt_pub_base_topic + config.PARAMS_CODES[125], str(power))
        else:
            command = {'sn': self._sn, 'par':[[125,7,str(power)]]}
            logger.debug(f"Welrok device {self._id} send_command_http = {command}")
            await self.send_command_http(command)

    async def set_temp(self, temp: int):
        if 5 <= temp <= 45:
            if self._mqtt_enable:
                self._mqtt.publish(self._mqtt_pub_base_topic + config.PARAMS_CODES[31], str(temp))
                self._mqtt.publish(self._mqtt_pub_base_topic + config.PARAMS_CODES[2], str(config.MODE_CODES_REVERSE['Manual']))
            else:
                logger.debug(f"Welrok device {self._id} send_command_http send_temp = {temp}")
                await self.send_command_http(
                    {'sn': self._sn, 'par':[
                            [2,2,str(config.MODE_CODES_REVERSE['Manual'])],
                            [31,3,str(temp * 10)]
                        ]
                    }
                )

    async def set_mode(self, new_mode: str):
        mode = [config.MODE_CODES_REVERSE.get(i) for i in new_mode.split('/') if config.MODE_CODES_REVERSE.get(i)].__iter__().__next__()
        if len(mode) > 0:
            if self._mqtt_enable:
                self._mqtt.publish(self._mqtt_pub_base_topic + config.PARAMS_CODES[2], str(mode[0]))
            else:
                await self.send_command_http({'sn': self._sn, 'par':[[2,2,str(mode[0])]]})

    async def set_bright(self, bright: int):
        if 0 <= bright <= 10:
            if self._mqtt_enable:
                self._mqtt.publish(self._mqtt_pub_base_topic + config.PARAMS_CODES[23], str(bright))
            else:
                await self.send_command_http({'sn': self._sn, 'par':[[23,2,str(bright)]]})


class WelrokClient:
    def __init__(self, devices_config):
        self.mqtt_client_running = False
        self.devices_config = devices_config

    async def _exit_gracefully(self):
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _on_mqtt_client_connect(self, _, __, ___, rc):
        if rc == 0:
            self.mqtt_client_running = True
            logger.info("MQTT client connected")

    def _on_mqtt_client_disconnect(self, _, userdata, __):
        self.mqtt_client_running = False
        asyncio.run_coroutine_threadsafe(self._exit_gracefully(), userdata)
        logger.info("MQTT client disconnected")

    def _on_term_signal(self):
        asyncio.create_task(self._exit_gracefully())
        logger.info("SIGTERM or SIGINT received, exiting")

    async def run(self):
        welrok_devices = []
        mqtt_devices = []

        try:
            event_loop = asyncio.get_event_loop()
            event_loop.add_signal_handler(signal.SIGTERM, self._on_term_signal)
            event_loop.add_signal_handler(signal.SIGINT, self._on_term_signal)
            mqtt_client = MQTTClient("welrok", DEFAULT_BROKER_URL)
            mqtt_client.user_data_set(event_loop)
            mqtt_client.on_connect = self._on_mqtt_client_connect
            mqtt_client.on_disconnect = self._on_mqtt_client_disconnect
            mqtt_client.start()

            logger.debug("MQTT client started")

            for device_config in self.devices_config:
                welrok_device = WelrokDevice(device_config)
                logger.debug(f"Welrok_device: {welrok_device}")
                if welrok_device._url:
                    logger.debug("Device start parse params")
                    logger.debug(f"CMD CODES: {config.CMD_CODES}")
                    params_states = await welrok_device.get_device_state(config.CMD_CODES['params'])
                    device_controls_state = welrok_device.parse_device_params_state(params_states)
                    logger.debug(f"Device controls state: {device_controls_state}")
                    telemetry = await welrok_device.get_device_state(config.CMD_CODES['telemetry'])
                    logger.debug(f"Telemetry: {telemetry}")
                    device_controls_state.update({
                        'read_only_temp': welrok_device.parse_temperature_response(telemetry),
                        'load': welrok_device.get_load(telemetry)
                    })
                    logger.debug(f"Device controls state after update: {device_controls_state}")
                    mqtt_device = MQTTDevice(mqtt_client, device_controls_state)
                    logger.debug(f"MQTTDevice: {mqtt_device}")
                    welrok_devices.append(asyncio.create_task(welrok_device.run()))
                    logger.debug(f"Welrok devices list: {welrok_devices}")
                    mqtt_devices.append(mqtt_device)
                    logger.debug(f"MQTTDevices list: {mqtt_devices}")
                    mqtt_device.set_welrok_device(welrok_device)
                    welrok_device.set_mqtt_device(mqtt_device)
                    mqtt_device.publicate()

            for welrok_device in welrok_devices:
                await welrok_device

        except (ConnectionError, ConnectionRefusedError) as e:
            logger.error("MQTT error connection to broker %s: %s", DEFAULT_BROKER_URL, e)
            return 1
        except asyncio.CancelledError:
            logger.debug("Run welrok client task cancelled")
            return 0 if self.mqtt_client_running else 1
        except Exception as e:
            logger.debug(f'Error: {e}.\n{traceback.format_exc()}')
        finally:

            if self.mqtt_client_running:
                for mqtt_device in mqtt_devices:
                    mqtt_device.remove()

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
                raise ValueError("Device ID's must be unique")

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