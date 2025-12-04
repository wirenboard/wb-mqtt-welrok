import asyncio
import logging
import signal
import traceback

from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient
from wb_welrok.wb_mqtt_device import MQTTDevice
from wb_welrok.wb_welrok_device import WelrokDevice

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s (%(filename)s:%(lineno)d)")
logger.setLevel(logging.INFO)


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

        except (ConnectionError, ConnectionRefusedError, OSError) as e:
            logger.error(
                "MQTT error connection to broker %s: %s", self.mqtt_server_uri or DEFAULT_BROKER_URL, e
            )
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
            # Wait for cancelled tasks to finish cleanup
            await asyncio.gather(
                *[entry["task"] for entry in active_devices.values() if not entry["task"].done()],
                return_exceptions=True,
            )
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
