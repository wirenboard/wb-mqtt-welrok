import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from wb_welrok.device_config_manager import ConfigManager
from wb_welrok.main import main
from wb_welrok.schemas import DeviceConfig
from wb_welrok.wb_welrok_client import WelrokClient
from wb_welrok.wbmqtt import ControlMeta, Device


def make_config(devices=None, broker="tcp://127.0.0.1:1883"):
    return SimpleNamespace(devices=devices or [], mqtt_server_uri=broker, debug=False)


def test_empty_configuration_exits_with_status_7():
    configured = make_config([DeviceConfig()])
    with patch("wb_welrok.main.ConfigManager") as manager, patch("wb_welrok.main.setup_logging"):
        manager.return_value.load_and_validate.return_value = configured

        assert main(["wb-welrok"]) == 7


def test_unreadable_configuration_exits_with_status_6(tmp_path):
    with patch("wb_welrok.main.config.SCHEMA_FILEPATH", str(tmp_path / "schema.json")):
        assert main(["wb-welrok", "-c", str(tmp_path)]) == 6


def test_invalid_broker_is_rejected_by_config_manager(tmp_path):
    schema_path = Path(__file__).parents[1] / "wb-mqtt-welrok.schema.json"
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": [],
                "debug": False,
                "mqtt_server_uri": "tcp://localhost",
            }
        ),
        encoding="utf-8",
    )

    assert ConfigManager(str(config_path), str(schema_path)).load_and_validate() is None


def test_device_republishes_metadata_values_and_subscriptions():
    mqtt = MagicMock()
    mqtt.publish.return_value.rc = 0
    device = Device(mqtt, "thermostat", "Thermostat", "wb-mqtt-welrok")
    callback = MagicMock()
    device.create_control("Power", ControlMeta(control_type="switch"), "1")
    device.add_control_message_callback("Power", callback)
    mqtt.reset_mock()
    mqtt.publish.return_value.rc = 0

    device.republish()

    mqtt.publish.assert_any_call("/devices/thermostat/meta/name", "Thermostat", retain=True)
    mqtt.publish.assert_any_call("/devices/thermostat/controls/Power", "1", retain=True)
    mqtt.subscribe.assert_called_once_with("/devices/thermostat/controls/Power/on")
    mqtt.message_callback_add.assert_called_once_with("/devices/thermostat/controls/Power/on", callback)


def test_root_mqtt_reconnect_republishes_active_devices():
    client = WelrokClient(make_config([DeviceConfig(device_id="thermostat")]))
    welrok = MagicMock()
    client.active_devices["thermostat"] = {"task": MagicMock(), "welrok": welrok}

    client._handle_mqtt_connect(0)
    client._handle_mqtt_disconnect(1)
    client._handle_mqtt_connect(0)

    welrok.republish.assert_called_once_with()
    assert client.mqtt_client_running is True


class RefusingMQTTClient:
    instance = None

    def __init__(self, *_args, **_kwargs):
        self.userdata = None
        self.stopped = False
        RefusingMQTTClient.instance = self

    def user_data_set(self, userdata):
        self.userdata = userdata

    def start(self):
        self.on_connect(self, self.userdata, {}, 5)

    def stop(self):
        self.stopped = True


def test_authentication_refusal_exits_with_status_2():
    async def run_client():
        client = WelrokClient(make_config([DeviceConfig(device_id="thermostat")]))
        with patch("wb_welrok.wb_welrok_client.MQTTClient", RefusingMQTTClient):
            assert await asyncio.wait_for(client.run(), timeout=1) == 2

    asyncio.run(run_client())
    assert RefusingMQTTClient.instance.stopped is True


def test_removing_unavailable_device_closes_session():
    async def remove_device():
        client = WelrokClient(make_config())
        task = asyncio.create_task(asyncio.sleep(10))
        welrok = MagicMock()
        welrok._wb_mqtt_device = None
        welrok.close_session = AsyncMock()
        client.active_devices["thermostat"] = {"task": task, "welrok": welrok}

        await client.remove_device("thermostat")

        assert task.cancelled()
        welrok.close_session.assert_awaited_once_with()

    asyncio.run(remove_device())
