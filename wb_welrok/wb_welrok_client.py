import asyncio
import logging
from typing import Dict, Optional, Set, TypedDict

from wb_welrok.mqtt_client import MQTTClient
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
        self.mqtt_client: Optional[MQTTClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._connected_event: Optional[asyncio.Event] = None
        self._stop_requested = False
        self._exit_code = 7
        self._ever_connected = False
        self._outage_logged = False

    def request_stop(self, exit_code: int = 7) -> None:
        if exit_code == 2:
            self._exit_code = 2
        self._stop_requested = True
        if self._stop_event is not None:
            self._stop_event.set()

    def _schedule_in_loop(self, callback, *args) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(callback, *args)

    def _handle_mqtt_connect(self, rc: int) -> None:
        if rc == 0:
            reconnect = self._ever_connected
            self._ever_connected = True
            self.mqtt_client_running = True
            if self._connected_event is not None:
                self._connected_event.set()
            if reconnect or self._outage_logged:
                logger.info("MQTT broker connection restored")
                for entry in self.active_devices.values():
                    entry["welrok"].republish()
            else:
                logger.info("MQTT client connected")
            self._outage_logged = False
            return

        self.mqtt_client_running = False
        if rc in (4, 5):
            self._outage_logged = True
            logger.error("MQTT authentication failed (rc=%s)", rc)
            self.request_stop(2)
        elif not self._outage_logged:
            self._outage_logged = True
            logger.warning("MQTT connection refused (rc=%s), retrying", rc)

    def _on_mqtt_client_connect(self, _, __, ___, rc):
        self._schedule_in_loop(self._handle_mqtt_connect, rc)

    def _handle_mqtt_disconnect(self, rc: int) -> None:
        self.mqtt_client_running = False
        if rc != 0 and not self._outage_logged:
            self._outage_logged = True
            logger.warning("MQTT broker connection lost (rc=%s), retrying", rc)

    def _on_mqtt_client_disconnect(self, _, __, rc):
        self._schedule_in_loop(self._handle_mqtt_disconnect, rc)

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
                if not t.cancelled() and t.exception() is not None:
                    logger.error("Device task %s failed: %s", device_id, t.exception())

            task.add_done_callback(done_callback)
        except Exception:
            logger.exception("Failed to initialize device %s", device_id)
        finally:
            self.initializing.discard(device_id)

    async def remove_device(self, device_id):
        entry = self.active_devices.pop(device_id, None)
        if entry:
            task = entry["task"]
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            try:
                if entry["welrok"]._wb_mqtt_device is not None:
                    entry["welrok"]._wb_mqtt_device.remove()
            except Exception:
                logger.exception("Error removing mqtt device %s", device_id)
            try:
                await entry["welrok"].close_session()
            except Exception:
                logger.exception("Error closing HTTP session for device %s", device_id)

    async def monitor_devices(self):
        while True:
            try:
                configured_ids = {d.device_id for d in self.devices_config.devices}
                for device_config in self.devices_config.devices:
                    device_id = device_config.get("device_id")
                    if device_id and (
                        device_id not in self.active_devices or self.active_devices[device_id]["task"].done()
                    ):
                        await self.init_device(device_config)

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
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        if self._stop_requested:
            self._stop_event.set()

        self.mqtt_client = MQTTClient("welrok", self.mqtt_server_uri)
        self.mqtt_client.user_data_set(self)
        self.mqtt_client.on_connect = self._on_mqtt_client_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_client_disconnect
        self.mqtt_client.start()

        try:
            connected_task = asyncio.create_task(self._connected_event.wait())
            stop_task = asyncio.create_task(self._stop_event.wait())
            done, pending = await asyncio.wait(
                (connected_task, stop_task), timeout=5.0, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if stop_task in done:
                return self._exit_code
            if not done:
                self._outage_logged = True
                logger.warning("MQTT broker is unavailable, retrying")

            self.monitor_task = asyncio.create_task(self.monitor_devices())
            stop_task = asyncio.create_task(self._stop_event.wait())
            done, pending = await asyncio.wait(
                (self.monitor_task, stop_task), return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if self.monitor_task in done and not self._stop_event.is_set():
                await self.monitor_task
                logger.error("Device monitor stopped unexpectedly")
                self._exit_code = 1
        finally:
            if self.monitor_task and not self.monitor_task.done():
                self.monitor_task.cancel()
                await asyncio.gather(self.monitor_task, return_exceptions=True)
            for dev_id in list(self.active_devices.keys()):
                await self.remove_device(dev_id)
            if self.mqtt_client is not None:
                self.mqtt_client.stop()
            logger.info("MQTT client stopped")
        return self._exit_code
