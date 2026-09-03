import logging
import random
import string
from urllib.parse import urlparse

import paho_socket

MQTT_KEEPALIVE = 30  # seconds
MQTT_RECONNECT_MIN_DELAY = 1  # seconds
MQTT_RECONNECT_MAX_DELAY = 120  # seconds

logger = logging.getLogger(__name__)


class MQTTClient(paho_socket.Client):
    def __init__(self, client_id_prefix: str, broker_url: str, is_threaded: bool = True):
        self._broker_url = urlparse(broker_url)
        self._is_threaded = is_threaded
        self._network_loop_started = False
        client_id = self.generate_client_id(client_id_prefix)
        transport = "websockets" if self._broker_url.scheme == "ws" else "tcp"
        super().__init__(client_id=client_id, transport=transport)

    @staticmethod
    def generate_client_id(client_id_prefix: str, suffix_length: int = 8) -> str:
        random_suffix = "".join(random.sample(string.ascii_letters + string.digits, suffix_length))
        return "%s-%s" % (client_id_prefix, random_suffix)

    @staticmethod
    def validate_broker_url(broker_url: str) -> None:
        parsed = urlparse(broker_url)
        if parsed.scheme == "unix":
            if not parsed.path:
                raise ValueError("MQTT UNIX socket path is empty")
            return
        if parsed.scheme not in ("mqtt-tcp", "tcp", "ws"):
            raise ValueError(f"Unknown MQTT URL scheme: {parsed.scheme or '<empty>'}")
        if parsed.hostname is None:
            raise ValueError("MQTT hostname is empty")
        if parsed.port is None:
            raise ValueError("MQTT port is missing")

    def start(self) -> None:
        self.validate_broker_url(self._broker_url.geturl())
        scheme = self._broker_url.scheme

        if self._broker_url.username:
            self.username_pw_set(self._broker_url.username, self._broker_url.password)

        if scheme == "ws" and self._broker_url.path:
            self.ws_set_options(self._broker_url.path)

        self.setup_reconnect()

        if self._is_threaded:
            if scheme == "unix":
                self.sock_connect_async(self._broker_url.path, keepalive=MQTT_KEEPALIVE)
            else:
                self.connect_async(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)
            self.loop_start()
            self._network_loop_started = True
        elif scheme == "unix":
            self.sock_connect(self._broker_url.path, keepalive=MQTT_KEEPALIVE)
        else:
            self.connect(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)

    def stop(self) -> None:
        try:
            self.disconnect()
        except Exception:
            logger.exception("Error during MQTT disconnect")
        if self._is_threaded and self._network_loop_started:
            self.loop_stop()
            self._network_loop_started = False

    def setup_reconnect(self, min_delay=MQTT_RECONNECT_MIN_DELAY, max_delay=MQTT_RECONNECT_MAX_DELAY):
        super().reconnect_delay_set(min_delay=min_delay, max_delay=max_delay)
