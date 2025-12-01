import logging
import random
import string
from urllib.parse import urlparse

import paho_socket

# DEFAULT_BROKER_URL = "unix:///var/run/mosquitto/mosquitto.sock"
DEFAULT_BROKER_URL = "tcp://localhost:1883"

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
        try:
            if scheme == "unix":
                self.sock_connect(self._broker_url.path)
            elif scheme in ["mqtt-tcp", "tcp", "ws"]:
                self.connect(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)
            else:
                raise Exception("Unkown mqtt url scheme: " + scheme)
        except Exception:
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
