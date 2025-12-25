import pytest
from unittest.mock import Mock, patch, MagicMock
from wb_welrok.mqtt_client import MQTTClient, DEFAULT_BROKER_URL


class TestMQTTClient:
    """Tests for MQTTClient"""


def _fake_paho_init(self, *args, **kwargs):
    # ensure attributes accessed by paho.Client.__del__ exist
    self._sock = None
    return None

    def test_client_id_generation(self):
        """Test generation of a unique client_id"""
        client_id1 = MQTTClient.generate_client_id("test")
        client_id2 = MQTTClient.generate_client_id("test")

        assert client_id1.startswith("test-")
        assert client_id2.startswith("test-")
        assert client_id1 != client_id2
        assert len(client_id1.split("-")[1]) == 8

    def test_client_id_custom_suffix_length(self):
        """Test client_id generation with a custom suffix length"""
        client_id = MQTTClient.generate_client_id("test", suffix_length=12)
        assert len(client_id.split("-")[1]) == 12

    @patch("wb_welrok.mqtt_client.paho_socket.Client.__init__")
    def test_init_with_default_broker(self, mock_init):
        """Test initialization with the default broker"""
        mock_init.side_effect = _fake_paho_init

        client = MQTTClient("test_client")

        # Default broker is a Unix domain socket
        assert client._broker_url.scheme == "unix"
        assert client._broker_url.path == "/var/run/mosquitto/mosquitto.sock"
        assert client._is_threaded is True

    @patch("wb_welrok.mqtt_client.paho_socket.Client.__init__")
    def test_init_with_custom_broker(self, mock_init):
        """Test initialization with a custom broker"""
        mock_init.side_effect = _fake_paho_init

        client = MQTTClient("test_client", "tcp://mqtt.example.com:8883")

        assert client._broker_url.scheme == "tcp"
        assert client._broker_url.hostname == "mqtt.example.com"
        assert client._broker_url.port == 8883

    @patch("wb_welrok.mqtt_client.paho_socket.Client.__init__")
    def test_init_with_credentials(self, mock_init):
        """Test initialization with username and password"""
        mock_init.side_effect = _fake_paho_init

        client = MQTTClient("test_client", "tcp://user:pass@localhost:1883")

        assert client._broker_url.username == "user"
        assert client._broker_url.password == "pass"

    @patch("wb_welrok.mqtt_client.paho_socket.Client.__init__")
    def test_init_websockets(self, mock_init):
        """Test initialization with websockets transport"""
        mock_init.side_effect = _fake_paho_init

        client = MQTTClient("test_client", "ws://localhost:9001/mqtt")

        assert client._broker_url.scheme == "ws"
        assert client._broker_url.path == "/mqtt"

    def test_start_tcp_connection(self):
        """Test starting with a TCP connection"""
        client = MQTTClient("test_client", "tcp://localhost:1883")

        with patch.object(client, "loop_start") as mock_loop_start, patch.object(
            client, "connect"
        ) as mock_connect, patch.object(client, "setup_reconnect") as mock_setup_reconnect:

            client.start()

            mock_loop_start.assert_called_once()
            mock_connect.assert_called_once_with("localhost", 1883, keepalive=30)
            mock_setup_reconnect.assert_called_once()

    def test_start_with_credentials(self):
        """Test starting with credentials"""
        client = MQTTClient("test_client", "tcp://user:pass@localhost:1883")

        with patch.object(client, "username_pw_set") as mock_auth, patch.object(
            client, "loop_start"
        ) as mock_loop_start, patch.object(client, "connect") as mock_connect, patch.object(
            client, "reconnect_delay_set"
        ) as mock_reconnect:

            client.start()

            mock_auth.assert_called_once_with("user", "pass")
            mock_loop_start.assert_called_once()
            mock_connect.assert_called_once()

    def test_start_websockets_with_path(self):
        """Test starting with websockets and a path"""
        client = MQTTClient("test_client", "ws://localhost:9001/mqtt")

        with patch.object(client, "ws_set_options") as mock_ws, patch.object(
            client, "loop_start"
        ) as mock_loop_start, patch.object(client, "connect") as mock_connect, patch.object(
            client, "reconnect_delay_set"
        ) as mock_reconnect:

            client.start()

            mock_ws.assert_called_once_with("/mqtt")
            mock_loop_start.assert_called_once()
            mock_connect.assert_called_once()

    def test_start_unix_socket(self):
        """Test starting with a Unix domain socket"""
        client = MQTTClient("test_client", "unix:///var/run/mosquitto.sock")

        with patch.object(client, "sock_connect") as mock_sock, patch.object(
            client, "loop_start"
        ) as mock_loop_start, patch.object(client, "reconnect_delay_set") as mock_reconnect:

            client.start()

            mock_sock.assert_called_once_with("/var/run/mosquitto.sock")
            mock_loop_start.assert_called_once()

    def test_start_unknown_scheme(self):
        """Test starting with an unknown URL scheme"""
        client = MQTTClient("test_client", "unknown://localhost:1883")

        with patch.object(client, "loop_start") as mock_loop_start, patch.object(
            client, "loop_stop"
        ) as mock_loop_stop, patch.object(client, "reconnect_delay_set") as mock_reconnect:

            with pytest.raises(Exception, match="Unkown mqtt url scheme"):
                client.start()

            # Verify that loop_stop is called on error
            mock_loop_stop.assert_called_once()

    def test_start_connection_failure(self):
        """Test handling of connection failure"""
        client = MQTTClient("test_client", "tcp://localhost:1883")

        with patch.object(client, "loop_start") as mock_loop_start, patch.object(
            client, "loop_stop"
        ) as mock_loop_stop, patch.object(
            client, "connect", side_effect=ConnectionRefusedError("Connection refused")
        ), patch.object(
            client, "reconnect_delay_set"
        ) as mock_reconnect:

            with pytest.raises(ConnectionRefusedError):
                client.start()

            mock_loop_stop.assert_called_once()

    def test_stop(self):
        """Test stopping the client"""
        client = MQTTClient("test_client")

        with patch.object(client, "disconnect") as mock_disconnect, patch.object(
            client, "loop_stop"
        ) as mock_loop_stop:

            client.stop()

            mock_disconnect.assert_called_once()
            mock_loop_stop.assert_called_once()

    def test_stop_with_disconnect_error(self):
        """Test stopping the client when disconnect raises an error"""
        client = MQTTClient("test_client")

        with patch.object(client, "disconnect", side_effect=RuntimeError("Disconnect error")), patch.object(
            client, "loop_stop"
        ) as mock_loop_stop:

            # Should swallow the exception and continue
            client.stop()

            mock_loop_stop.assert_called_once()

    def test_stop_non_threaded(self):
        """Test stopping a non-threaded client"""
        client = MQTTClient("test_client", is_threaded=False)

        with patch.object(client, "disconnect") as mock_disconnect, patch.object(
            client, "loop_stop"
        ) as mock_loop_stop:

            client.stop()

            mock_disconnect.assert_called_once()
            # loop_stop should not be called for non-threaded clients
            mock_loop_stop.assert_not_called()

    def test_setup_reconnect(self):
        """Test setup for automatic reconnects"""
        client = MQTTClient("test_client")

        # Mock the parent class method
        with patch.object(client.__class__.__bases__[0], "reconnect_delay_set") as mock_reconnect:
            client.setup_reconnect(min_delay=2, max_delay=60)
            mock_reconnect.assert_called_once_with(min_delay=2, max_delay=60)

    def test_setup_reconnect_default_values(self):
        """Test reconnect setup with default values"""
        client = MQTTClient("test_client")

        with patch.object(client.__class__.__bases__[0], "reconnect_delay_set") as mock_reconnect:
            client.setup_reconnect()
            mock_reconnect.assert_called_once_with(min_delay=1, max_delay=120)


class TestMQTTClientIntegration:
    """Integration tests for the MQTT client"""

    @pytest.mark.skip(reason="Requires a running MQTT broker")
    def test_real_connection(self):
        """Test real broker connection"""
        import time

        client = MQTTClient("integration_test", "tcp://localhost:1883")

        connected = False

        def on_connect(client, userdata, flags, rc):
            nonlocal connected
            connected = rc == 0

        client.on_connect = on_connect
        client.start()

        # Wait for connection
        for _ in range(50):
            if connected:
                break
            time.sleep(0.1)

        assert connected, "Failed to connect to broker"

        client.stop()

    @pytest.mark.skip(reason="Requires a running MQTT broker")
    def test_reconnect_on_disconnect(self):
        """Test automatic reconnect on disconnect"""
        import time

        client = MQTTClient("reconnect_test", "tcp://localhost:1883")

        connect_count = 0

        def on_connect(client, userdata, flags, rc):
            nonlocal connect_count
            connect_count += 1

        client.on_connect = on_connect
        client.start()

        # Wait for the initial connection
        time.sleep(1)
        initial_count = connect_count

        # Simulate a disconnect
        client.disconnect()

        # Wait for reconnection
        time.sleep(3)

        assert connect_count > initial_count, "Client did not reconnect"

        client.stop()
