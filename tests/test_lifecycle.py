"""
Service lifecycle: exit codes, the MQTT connection retry, reconnect recovery and shutdown.

No broker or thermostat is involved: paho is replaced by MagicMock, the config goes through the
real schema shipped in the repository.
"""

# pylint: disable=protected-access, redefined-outer-name

import asyncio
import functools
import json
import pathlib
import signal
from unittest.mock import MagicMock, patch

import pytest

from wb_welrok import main as main_module
from wb_welrok import wbmqtt
from wb_welrok.device_config_manager import ConfigManager
from wb_welrok.mqtt_client import MQTTClient, validate_broker_url
from wb_welrok.wb_welrok_client import EXIT_INVALIDARGUMENT, EXIT_SUCCESS, WelrokClient

SCHEMA_PATH = str(pathlib.Path(__file__).resolve().parents[1] / "wb-mqtt-welrok.schema.json")
STUB_DEVICE = {
    "device_id": "",
    "device_title": "Welrok_thermostat",
    "device_ip": "",
    "inner_mqtt_client_id": "",
    "inner_mqtt_pubprefix": "",
    "inner_mqtt_subprefix": "",
    "serial_number": "",
    "mqtt_enable": False,
}
REAL_DEVICE = {**STUB_DEVICE, "device_id": "welrok_1", "device_ip": "192.168.1.10", "serial_number": "SN1"}


def write_config(tmp_path, devices, mqtt_server_uri="tcp://localhost:1883", text=None):
    path = tmp_path / "wb-welrok.conf"
    content = (
        text
        if text is not None
        else json.dumps({"devices": devices, "debug": False, "mqtt_server_uri": mqtt_server_uri})
    )
    path.write_text(content, encoding="utf-8")
    return str(path)


@pytest.mark.parametrize(
    "url", ["unix:///var/run/mosquitto/mosquitto.sock", "tcp://user:pw@localhost:1883", "ws://host:9001/mqtt"]
)
def test_valid_broker_urls_pass(url):
    validate_broker_url(url)


@pytest.mark.parametrize("url", ["http://localhost:1883", "tcp://localhost", "unix://", "localhost:1883"])
def test_invalid_broker_urls_raise(url):
    with pytest.raises(ValueError):
        validate_broker_url(url)


def test_stub_devices_without_an_id_are_skipped(tmp_path):
    manager = ConfigManager(
        write_config(tmp_path, [STUB_DEVICE, REAL_DEVICE]), SCHEMA_PATH
    ).load_and_validate()
    assert [device.device_id for device in manager.devices] == ["welrok_1"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"devices": [REAL_DEVICE], "mqtt_server_uri": "tcp://localhost"},
        {"devices": [REAL_DEVICE, REAL_DEVICE]},
        {"devices": [], "text": "{broken"},
        {"devices": [], "text": "[]"},
        {"devices": [{"device_id": "x"}]},
    ],
    ids=["broker-url-without-port", "duplicate-ids", "broken-json", "not-an-object", "missing-fields"],
)
def test_unusable_config_returns_none(tmp_path, kwargs):
    assert ConfigManager(write_config(tmp_path, **kwargs), SCHEMA_PATH).load_and_validate() is None


def test_missing_config_returns_none(tmp_path):
    assert ConfigManager(str(tmp_path / "absent.conf"), SCHEMA_PATH).load_and_validate() is None


def test_main_exit_codes(tmp_path, monkeypatch):
    monkeypatch.setattr(main_module.config, "SCHEMA_FILEPATH", SCHEMA_PATH)
    assert (
        main_module.main(["wb-welrok", "-c", write_config(tmp_path, [STUB_DEVICE])])
        == main_module.EXIT_NOTRUNNING
    )
    assert (
        main_module.main(["wb-welrok", "-c", str(tmp_path / "absent.conf")]) == main_module.EXIT_NOTCONFIGURED
    )
    assert (
        main_module.main(["wb-welrok", "-j", "-c", str(tmp_path / "absent.conf")])
        == main_module.EXIT_NOTCONFIGURED
    )
    with pytest.raises(SystemExit) as exit_info:
        main_module.main(["wb-welrok", "--bogus"])
    assert exit_info.value.code == 2


def test_main_returns_the_exit_code_of_the_client(tmp_path, monkeypatch):
    monkeypatch.setattr(main_module.config, "SCHEMA_FILEPATH", SCHEMA_PATH)
    with patch("wb_welrok.main.WelrokClient") as client_class:
        client_class.return_value.run = MagicMock(return_value=_completed(EXIT_INVALIDARGUMENT))
        assert (
            main_module.main(["wb-welrok", "-c", write_config(tmp_path, [REAL_DEVICE])])
            == EXIT_INVALIDARGUMENT
        )


async def _completed(value):
    return value


@pytest.mark.parametrize(
    "url, connect_method, args",
    [
        ("unix:///run/mosquitto.sock", "sock_connect_async", ("/run/mosquitto.sock",)),
        ("tcp://localhost:1883", "connect_async", ("localhost", 1883)),
    ],
)
def test_start_connects_in_the_network_thread(url, connect_method, args):
    """
    Nothing connects synchronously: paho's thread retries an unavailable broker with the
    configured delays instead of a bounded loop that gave up after a few attempts.
    """
    client = MQTTClient("test", url)
    with (
        patch.object(client, connect_method) as connect,
        patch.object(client, "loop_start") as loop_start,
        patch("wb_welrok.mqtt_client.paho_socket.Client.reconnect_delay_set") as reconnect_delay_set,
    ):
        client.start()
    connect.assert_called_once_with(*args, keepalive=30)
    loop_start.assert_called_once_with()
    reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=120)


def _client_with_fake_mqtt(monkeypatch, connack=0):
    fake_mqtt = MagicMock()
    fake_mqtt.is_connected.return_value = connack == 0
    client = WelrokClient(MagicMock(mqtt_server_uri="tcp://localhost:1883", devices=[]))
    fake_mqtt.start.side_effect = lambda: client._on_mqtt_client_connect(fake_mqtt, None, None, connack)
    monkeypatch.setattr("wb_welrok.wb_welrok_client.MQTTClient", lambda *_args, **_kwargs: fake_mqtt)
    monkeypatch.setattr("wb_welrok.wb_welrok_client.wbmqtt.retain_hack", lambda _client: None)
    return client, fake_mqtt


@pytest.fixture
def signal_handlers(monkeypatch):
    """
    Record the signal handlers run() installs instead of letting them land on the pytest process.
    """
    handlers = {}

    def record(_loop, signum, callback, *args):
        handlers[signum] = functools.partial(callback, *args)

    monkeypatch.setattr(asyncio.SelectorEventLoop, "add_signal_handler", record)
    return handlers


@pytest.mark.asyncio
@pytest.mark.usefixtures("signal_handlers")
async def test_rejected_login_exits_with_2(monkeypatch):
    client, fake_mqtt = _client_with_fake_mqtt(monkeypatch, connack=5)

    assert await client.run() == EXIT_INVALIDARGUMENT
    fake_mqtt.stop.assert_called_once_with()
    assert client.monitor_task is None  # no device was served on a rejected login


@pytest.mark.asyncio
async def test_signal_stops_the_client_with_0(monkeypatch, signal_handlers):
    client, fake_mqtt = _client_with_fake_mqtt(monkeypatch)
    asyncio.get_running_loop().call_later(0.05, client._on_term_signal)

    assert await client.run() == EXIT_SUCCESS
    assert set(signal_handlers) == {signal.SIGTERM, signal.SIGINT}
    assert {handler.func for handler in signal_handlers.values()} == {client._on_term_signal}
    fake_mqtt.stop.assert_called_once_with()
    assert client.monitor_task.done()  # monitor_devices swallows its own cancellation


@pytest.mark.asyncio
async def test_lost_connection_is_not_a_stop(monkeypatch):
    client, _ = _client_with_fake_mqtt(monkeypatch)
    client._on_mqtt_client_disconnect(None, None, 1)
    assert not client._stop.is_set()


@pytest.mark.asyncio
async def test_reconnect_republishes_every_active_device():
    client = WelrokClient(MagicMock(mqtt_server_uri="tcp://localhost:1883", devices=[]))
    client._loop = asyncio.get_running_loop()
    welrok = MagicMock()
    client.active_devices["d1"] = {"task": MagicMock(), "welrok": welrok}

    client._on_mqtt_client_connect(None, None, None, 0)
    await asyncio.sleep(0)

    welrok.republish_mqtt.assert_called_once_with()


def test_device_republish_restores_topics_and_command_subscriptions():
    mqtt = MagicMock()
    device = wbmqtt.Device(mqtt, "welrok_1", "Thermostat", "wb-mqtt-welrok")
    device.create_control("Power", wbmqtt.ControlMeta(title="Power", control_type="switch"), "1")
    device.add_control_message_callback("Power", lambda *_: None)
    mqtt.reset_mock()

    device.republish()

    published = [call.args[0] for call in mqtt.publish.call_args_list]
    assert published == [
        "/devices/welrok_1/meta/name",
        "/devices/welrok_1/meta/driver",
        "/devices/welrok_1/controls/Power/meta",
        "/devices/welrok_1/controls/Power",
    ]
    mqtt.subscribe.assert_called_once_with("/devices/welrok_1/controls/Power/on")
