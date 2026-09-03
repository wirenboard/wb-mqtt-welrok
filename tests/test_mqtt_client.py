from unittest.mock import patch

import pytest

from wb_welrok.mqtt_client import MQTTClient


def test_client_id_is_unique():
    first = MQTTClient.generate_client_id("test")
    second = MQTTClient.generate_client_id("test")

    assert first.startswith("test-")
    assert second.startswith("test-")
    assert first != second


@pytest.mark.parametrize(
    "broker_url",
    ["", "tcp://", "tcp://localhost", "http://localhost:1883", "unix://"],
)
def test_invalid_broker_url_is_rejected(broker_url):
    with pytest.raises(ValueError):
        MQTTClient.validate_broker_url(broker_url)


def test_threaded_tcp_client_starts_nonblocking_connection():
    client = MQTTClient("test", "tcp://localhost:1883")

    with patch.object(client, "connect_async") as connect_async, patch.object(
        client, "loop_start"
    ) as loop_start:
        client.start()

    connect_async.assert_called_once_with("localhost", 1883, keepalive=30)
    loop_start.assert_called_once_with()


def test_threaded_unix_client_starts_nonblocking_connection():
    client = MQTTClient("test", "unix:///run/mosquitto.sock")

    with patch.object(client, "sock_connect_async") as connect_async, patch.object(
        client, "loop_start"
    ) as loop_start:
        client.start()

    connect_async.assert_called_once_with("/run/mosquitto.sock", keepalive=30)
    loop_start.assert_called_once_with()


def test_credentials_are_applied_before_connection():
    client = MQTTClient("test", "tcp://user:password@localhost:1883")

    with patch.object(client, "username_pw_set") as set_credentials, patch.object(
        client, "connect_async"
    ), patch.object(client, "loop_start"):
        client.start()

    set_credentials.assert_called_once_with("user", "password")


def test_stop_disconnects_before_stopping_network_loop():
    client = MQTTClient("test", "tcp://localhost:1883")
    client._network_loop_started = True
    calls = []

    with patch.object(client, "disconnect", side_effect=lambda: calls.append("disconnect")), patch.object(
        client, "loop_stop", side_effect=lambda: calls.append("loop_stop")
    ):
        client.stop()

    assert calls == ["disconnect", "loop_stop"]
