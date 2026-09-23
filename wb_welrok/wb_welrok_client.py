import asyncio
import logging
import signal
from typing import Dict, Optional, Set, TypedDict

from wb_welrok import wbmqtt
from wb_welrok.mqtt_client import MQTT_AUTH_ERRORS, MQTTClient
from wb_welrok.wb_welrok_device import WelrokDevice

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_INVALIDARGUMENT = 2


class DeviceEntry(TypedDict):
    task: asyncio.Task
    welrok: WelrokDevice


class WelrokClient:
    def __init__(self, devices_config):
        self.devices_config = devices_config
        self.mqtt_client_running = False
        self.mqtt_server_uri = devices_config.mqtt_server_uri
        self.mqtt_client: Optional[MQTTClient] = None
        self.active_devices: Dict[str, DeviceEntry] = {}
        self.initializing: Set[str] = set()
        self.monitor_task: Optional[asyncio.Task] = None
        self.exit_code = EXIT_SUCCESS
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = asyncio.Event()

    def stop(self, exit_code: int = EXIT_SUCCESS) -> None:
        """
        Ask run() to shut down. Call it from the event loop thread only.
        """
        self.exit_code = exit_code
        self._stop.set()

    def _on_term_signal(self):
        logger.info("Termination signal received, exiting")
        self.stop()

    def _on_mqtt_client_connect(self, _, __, ___, rc):
        # runs on paho's network thread: only hand work over to the event loop
        if rc != 0:
            logger.error("MQTT connect failed, rc=%s", rc)
            if rc in MQTT_AUTH_ERRORS:
                # a rejected login is a configuration problem paho would retry forever: exit with 2
                self._loop.call_soon_threadsafe(self.stop, EXIT_INVALIDARGUMENT)
            return
        self.mqtt_client_running = True
        logger.info("MQTT client connected")
        # WB service guideline: re-subscribe and republish the meta and the last control values in the
        # connect handler. paho re-subscribes nothing (clean session); the republish is cheap and
        # idempotent, so we do not tell a broker restart from a plain reconnect
        self._loop.call_soon_threadsafe(self._republish_devices)

    def _on_mqtt_client_disconnect(self, _, __, rc):
        self.mqtt_client_running = False
        if rc != 0:
            logger.warning("MQTT client disconnected (rc=%s), reconnecting", rc)

    def _republish_devices(self):
        for entry in self.active_devices.values():
            entry["welrok"].republish_mqtt()

    async def _wait_for_mqtt_connect(self):
        while not self.mqtt_client_running and not self._stop.is_set():
            await asyncio.sleep(0.1)

    async def init_device(self, device_config):
        device_id = device_config.get("device_id")
        if not device_id or device_id in self.initializing:
            return
        self.initializing.add(device_id)
        try:
            await self.remove_device(device_id)

            welrok_device = WelrokDevice(device_config, self.mqtt_server_uri, self.mqtt_client)
            task = asyncio.create_task(welrok_device.run())
            self.active_devices[device_id] = {"task": task, "welrok": welrok_device}

            def done_callback(t):
                logger.info("Device task %s finished", device_id)
                asyncio.create_task(self.remove_device(device_id))

            task.add_done_callback(done_callback)
        except Exception:
            logger.exception("Failed to initialize device %s", device_id)
        finally:
            self.initializing.discard(device_id)

    async def remove_device(self, device_id):
        entry = self.active_devices.pop(device_id, None)
        if not entry:
            return
        # the task first: its finally unsubscribes and stops the thermostat's MQTT client, and no
        # poll may publish into the WB device while it is being removed
        entry["task"].cancel()
        await asyncio.gather(entry["task"], return_exceptions=True)
        try:
            entry["welrok"].remove_mqtt_device()
            await entry["welrok"].close_session()
        except Exception:
            logger.exception("Error removing mqtt device %s", device_id)

    async def monitor_devices(self):
        while True:
            try:
                configured_ids = {d.device_id for d in self.devices_config.devices}
                for device_config in self.devices_config.devices:
                    device_id = device_config.get("device_id")
                    if device_id and (
                        device_id not in self.active_devices or self.active_devices[device_id]["task"].done()
                    ):
                        asyncio.create_task(self.init_device(device_config))

                for dev_id in list(self.active_devices.keys()):
                    if dev_id not in configured_ids:
                        await self.remove_device(dev_id)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.debug("monitor_devices cancelled")
                break
            except Exception:
                logger.exception("Error in monitor_devices loop")

    async def run(self) -> int:
        """
        Serve until SIGINT/SIGTERM or a rejected MQTT login; returns the exit code.
        """
        self._loop = asyncio.get_running_loop()
        self._loop.add_signal_handler(signal.SIGTERM, self._on_term_signal)
        self._loop.add_signal_handler(signal.SIGINT, self._on_term_signal)

        self.mqtt_client = MQTTClient("welrok", self.mqtt_server_uri)
        self.mqtt_client.on_connect = self._on_mqtt_client_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_client_disconnect
        self.mqtt_client.start()
        # the devices are published only on a live connection; paho retries the broker meanwhile
        await self._wait_for_mqtt_connect()

        if not self._stop.is_set():
            self.monitor_task = asyncio.create_task(self.monitor_devices())
            await self._stop.wait()
        await self._shutdown()
        return self.exit_code

    async def _shutdown(self):
        if self.monitor_task:
            self.monitor_task.cancel()
            await asyncio.gather(self.monitor_task, return_exceptions=True)
        for dev_id in list(self.active_devices.keys()):
            await self.remove_device(dev_id)
        if self.mqtt_client.is_connected():
            # the retained clears are QoS 0: the token round trip (bounded by its own timeout)
            # shows the broker has processed everything published before it
            await asyncio.to_thread(wbmqtt.retain_hack, self.mqtt_client)
        else:
            logger.error("MQTT broker is not connected, retained topics cannot be removed")
        self.mqtt_client.stop()
        logger.info("MQTT client stopped")
