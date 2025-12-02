import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from wb_welrok import config
from wb_welrok.main import MQTTDevice, WelrokClient, WelrokDevice


class TestWelrokDevice:
    """Test for WelrokDevice"""

    @pytest.fixture
    def device_config(self):
        return {
            "device_id": "test_device",
            "device_title": "Test Device",
            "serial_number": "SN123",
            "device_ip": "192.168.1.100",
            "mqtt_enable": False,
            "mqtt_server_uri": "tcp://localhost:1883",
            "inner_mqtt_pubprefix": "pub/",
            "inner_mqtt_client_id": "client1",
            "inner_mqtt_subprefix": "sub/",
        }

    @pytest.fixture
    def welrok_device(self, device_config):
        with patch("wb_welrok.main.MQTTClient"):
            device = WelrokDevice(device_config)
            return device

    def test_init(self, welrok_device, device_config):
        """Test WelrokDevice initialization"""
        assert welrok_device.id == device_config["device_id"]
        assert welrok_device.sn == device_config["serial_number"]
        assert welrok_device.title == device_config["device_title"]
        assert welrok_device.ip == device_config["device_ip"]
        assert welrok_device._url == "http://192.168.1.100/api.cgi"
        assert welrok_device._session is None

    def test_init_no_ip(self):
        """Test initialization without IP"""
        config_dict = {
            "device_id": "test",
            "device_title": "Test",
            "serial_number": "SN",
            "mqtt_server_uri": "tcp://localhost:1883",
        }
        with patch("wb_welrok.main.MQTTClient"):
            device = WelrokDevice(config_dict)
            assert device._url is None

    def test_parse_device_params_state(self, welrok_device):
        """Test parsing device parameters state"""
        data = {
            "par": [
                [125, 7, "0"],  # powerOff
                [23, 2, "5"],  # bright
                [31, 3, "250"],  # setTemp
            ]
        }

        result = welrok_device.parse_device_params_state(data)

        assert result is not None
        assert "powerOff" in result
        assert "bright" in result
        assert "setTemp" in result
        assert result["setTemp"] == 25.0  # 250 / 10

    def test_parse_temperature_response(self, welrok_device):
        """Test parsing temperature response"""
        data = {
            "t.1": "320",  # 320 / 16 = 20.0
            "t.2": "352",  # 352 / 16 = 22
        }

        result = welrok_device.parse_temperature_response(data)

        assert "Floor temperature" in result
        assert result["Floor temperature"] == "20.0"

    def test_get_load(self, welrok_device):
        """Test getting load status"""
        telemetry_on = {"f.0": "1"}
        telemetry_off = {"f.0": "0"}

        assert welrok_device.get_load(telemetry_on) == "Включено"
        assert welrok_device.get_load(telemetry_off) == "Выключено"
        assert welrok_device.get_load({}) == "off"


class TestMQTTDevice:
    """Tests for MQTTDevice"""

    @pytest.fixture
    def device_state(self):
        return {
            "powerOff": 0,
            "bright": 5,
            "setTemp": 22,
            "mode": "Manual",
            "load": "on",
            "read_only_temp": {
                "Floor temperature": "20.0",
                "Air temperature": "21.0",
            },
        }

    @pytest.fixture
    def mqtt_device(self, device_state):
        mock_client = MagicMock()
        with patch("asyncio.get_running_loop"):
            device = MQTTDevice(mock_client, device_state)
            return device

    def test_init(self, mqtt_device, device_state):
        """Test MQTTDevice initialization"""
        assert mqtt_device._device_state == device_state
        assert mqtt_device._device is None
        assert mqtt_device._welrok_device is None

    def test_set_welrok_device(self, mqtt_device):
        """Test attaching Welrok device"""
        mock_welrok = MagicMock()
        mock_welrok.title = "Test Device"
        mock_welrok.sn = "SN123"

        mqtt_device.set_welrok_device(mock_welrok)

        assert mqtt_device._welrok_device is mock_welrok
        assert mqtt_device._root_topic == "/devices/Test Device"

    def test_on_message_power_event_loop_closed(self, mqtt_device):
        """Test handling power message when event loop is closed"""
        mock_welrok = MagicMock()
        mock_welrok.sn = "SN123"
        mqtt_device._welrok_device = mock_welrok

        mock_loop = MagicMock()
        mock_loop.is_closed.return_value = True
        mqtt_device._loop = mock_loop

        msg = MagicMock()
        msg.payload.decode.return_value = "1"

        # Should not raise an exception
        mqtt_device._on_message_power(None, None, msg)

    def test_on_message_temperature_valid(self, mqtt_device):
        """Test handling a valid temperature"""
        mock_welrok = MagicMock()
        mock_welrok.sn = "SN123"
        mqtt_device._welrok_device = mock_welrok

        mock_loop = MagicMock()
        mock_loop.is_closed.return_value = False
        mqtt_device._loop = mock_loop

        msg = MagicMock()
        msg.payload.decode.return_value = "25"

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            mqtt_device._on_message_temperature(None, None, msg)

            mock_run.assert_called_once()


class TestWelrokClient:
    """Tests for WelrokClient"""

    @pytest.fixture
    def devices_config(self):
        return [
            {
                "device_id": "device1",
                "device_title": "Device 1",
                "serial_number": "SN001",
                "device_ip": "192.168.1.101",
                "mqtt_server_uri": "tcp://localhost:1883",
            }
        ]

    @pytest.fixture
    def welrok_client(self, devices_config):
        return WelrokClient(devices_config)

    def test_init(self, welrok_client, devices_config):
        """Test WelrokClient initialization"""
        assert welrok_client.devices_config == devices_config
        assert welrok_client.mqtt_server_uri == "tcp://localhost:1883"
        assert welrok_client.mqtt_client_running is False

    def test_on_mqtt_client_connect_success(self, welrok_client):
        """Test successful MQTT client connection"""
        welrok_client._on_mqtt_client_connect(None, None, None, 0)

        assert welrok_client.mqtt_client_running is True

    def test_on_mqtt_client_connect_failure(self, welrok_client):
        """Test failed MQTT client connection"""
        welrok_client._on_mqtt_client_connect(None, None, None, 1)

        assert welrok_client.mqtt_client_running is False

    def test_on_mqtt_client_disconnect_normal(self, welrok_client):
        """Test normal disconnection (rc=0)"""
        welrok_client.mqtt_client_running = True
        mock_loop = MagicMock()

        # rc=0 should not call _exit_gracefully
        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            welrok_client._on_mqtt_client_disconnect(None, mock_loop, 0)

            mock_run.assert_not_called()
            assert welrok_client.mqtt_client_running is False

    def test_on_mqtt_client_disconnect_error(self, welrok_client):
        """Test disconnection with error (rc!=0)"""
        welrok_client.mqtt_client_running = True
        mock_loop = MagicMock()

        # rc!=0 should call _exit_gracefully
        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            welrok_client._on_mqtt_client_disconnect(None, mock_loop, 1)

            mock_run.assert_called_once()
            assert welrok_client.mqtt_client_running is False
