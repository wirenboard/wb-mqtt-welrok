import asyncio
import logging
import signal
import traceback
from typing import Dict, Optional, Set, TypedDict

from wb_welrok import config
from wb_welrok.mqtt_client import MQTTClient
from wb_welrok.wb_mqtt_device import MQTTDevice
from wb_welrok.wb_welrok_device import WelrokDevice

logger = logging.getLogger(__name__)


class DeviceEntry(TypedDict):
    task: asyncio.Task
    welrok: WelrokDevice


class WelrokClient:
    def __init__(self, devices_config):
        self.devices_config = devices_config
        self.mqtt_client_running = False
        self.mqtt_server_uri = devices_config.mqtt_server_uri
        self.active_devices: Dict[str, DeviceEntry] = {}
        self.initializing: Set[str] = set()
        self.monitor_task: Optional[asyncio.Task] = None

    async def _exit_gracefully(self):
        logger.info("Cancelling all device tasks")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _on_term_signal(self):
        logger.info("Termination signal received, exiting")
        asyncio.create_task(self._exit_gracefully())

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
        if entry:
            try:
                entry["welrok"]._wb_mqtt_device.remove()
                await entry["welrok"].close_session()
                entry["task"].cancel()
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

    async def run(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._on_term_signal)
        loop.add_signal_handler(signal.SIGINT, self._on_term_signal)

        self.mqtt_client = MQTTClient("welrok", self.mqtt_server_uri)
        self.mqtt_client.user_data_set(self)
        self.mqtt_client.on_connect = self._on_mqtt_client_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_client_disconnect
        self.mqtt_client.start()

        try:
            await asyncio.wait_for(self._wait_for_mqtt_connect(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("MQTT client did not connect within timeout")

        self.monitor_task = asyncio.create_task(self.monitor_devices())

        try:
            await self.monitor_task
        except asyncio.CancelledError:
            logger.info("WelrokClient run cancelled")
        finally:
            if self.monitor_task:
                self.monitor_task.cancel()
            for dev_id in list(self.active_devices.keys()):
                await self.remove_device(dev_id)
            self.mqtt_client.stop()
            logger.info("MQTT client stopped")
