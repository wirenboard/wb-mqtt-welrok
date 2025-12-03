import logging
import random
import socket
import string
import time
from urllib.parse import urlparse

import paho_socket

# DEFAULT_BROKER_URL = "unix:///var/run/mosquitto/mosquitto.sock"
DEFAULT_BROKER_URL = "tcp://127.0.0.1:1883"

MQTT_KEEPALIVE = 30  # seconds
MQTT_RECONNECT_MIN_DELAY = 1  # seconds
MQTT_RECONNECT_MAX_DELAY = 120  # seconds


class MQTTClient(paho_socket.Client):
    def __init__(self, client_id_prefix: str, broker_url: str = DEFAULT_BROKER_URL, is_threaded: bool = True):
        self._broker_url = urlparse(broker_url)
        self._is_threaded = is_threaded
        client_id = self.generate_client_id(client_id_prefix)
        transport = "websockets" if self._broker_url.scheme == "ws" else "tcp"
        super().__init__(client_id=client_id, transport=transport)

    @staticmethod
    def generate_client_id(client_id_prefix: str, suffix_length: int = 8) -> str:
        random_suffix = "".join(random.sample(string.ascii_letters + string.digits, suffix_length))
        return "%s-%s" % (client_id_prefix, random_suffix)

    def start(self) -> None:
        scheme = self._broker_url.scheme

        if self._broker_url.username:
            self.username_pw_set(self._broker_url.username, self._broker_url.password)

        if scheme == "ws" and self._broker_url.path:
            self.ws_set_options(self._broker_url.path)

        # Setup automatic reconnection
        self.setup_reconnect()

        if self._is_threaded:
            self.loop_start()
        max_retries = 5
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                logging.getLogger(__name__).info(
                    f"Attempting MQTT connection to {self._broker_url.hostname}:{self._broker_url.port} (attempt {attempt + 1})"
                )
                # Test socket connectivity
                try:
                    test_sock = socket.create_connection(
                        (self._broker_url.hostname, self._broker_url.port), timeout=5
                    )
                    test_sock.close()
                    logging.getLogger(__name__).debug("Socket connectivity test passed")
                except Exception as sock_e:
                    logging.getLogger(__name__).warning(f"Socket connectivity test failed: {sock_e}")
                if scheme == "unix":
                    self.sock_connect(self._broker_url.path)
                elif scheme in ["mqtt-tcp", "tcp", "ws"]:
                    self.connect(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)
                else:
                    raise Exception("Unkown mqtt url scheme: " + scheme)
                logging.getLogger(__name__).info("MQTT connection successful")
                break  # Success, exit loop
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.getLogger(__name__).warning(
                        f"MQTT connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    logging.getLogger(__name__).error(
                        f"MQTT connection failed after {max_retries} attempts: {e}"
                    )
                    if self._is_threaded:
                        try:
                            self.loop_stop()
                        except Exception:
                            logging.getLogger(__name__).exception("Error stopping loop after failed start")
                    raise

    def stop(self) -> None:
        try:
            self.disconnect()
        except Exception:
            logging.getLogger(__name__).exception("Error during MQTT disconnect")
        if self._is_threaded:
            self.loop_stop()

    def setup_reconnect(self, min_delay=MQTT_RECONNECT_MIN_DELAY, max_delay=MQTT_RECONNECT_MAX_DELAY):
        super().reconnect_delay_set(min_delay=min_delay, max_delay=max_delay)
