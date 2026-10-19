import logging
import random
import string
from urllib.parse import urlparse

import paho_socket

MQTT_KEEPALIVE = 30  # seconds
MQTT_RECONNECT_MIN_DELAY = 1  # seconds
MQTT_RECONNECT_MAX_DELAY = 120  # seconds

# CONNACK codes for a rejected login: bad user name or password, not authorized
MQTT_AUTH_ERRORS = (4, 5)
BROKER_URL_SCHEMES = ("unix", "mqtt-tcp", "tcp", "ws")

logger = logging.getLogger(__name__)


def validate_broker_url(broker_url: str) -> None:
    """
    Raise ValueError for a URL no connection attempt could ever succeed with.

    The message never repeats the URL: it may carry a password (the config editor asks for
    tcp://user:password@host:1883), and the message ends up in the journal.
    """
    try:
        url = urlparse(broker_url)
        if url.scheme == "unix":
            if url.path:
                return
        elif url.scheme in BROKER_URL_SCHEMES and url.hostname and url.port:
            return
    except ValueError:
        pass
    raise ValueError("MQTT URL must be unix:///path or tcp://host:port (also mqtt-tcp://, ws://)")


class MQTTClient(paho_socket.Client):
    def __init__(self, client_id_prefix: str, broker_url: str, is_threaded: bool = True):
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
        """
        Connect from paho's network thread. An unavailable broker is retried with the delays of
        setup_reconnect() until stop(), and so is a lost connection; the URL was validated by the
        config loader, so nothing is raised here.
        """
        if self._broker_url.username:
            self.username_pw_set(self._broker_url.username, self._broker_url.password)

        if self._broker_url.scheme == "ws" and self._broker_url.path:
            self.ws_set_options(self._broker_url.path)

        self.setup_reconnect()

        if self._broker_url.scheme == "unix":
            self.sock_connect_async(self._broker_url.path, keepalive=MQTT_KEEPALIVE)
        else:
            self.connect_async(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)

        if self._is_threaded:
            self.loop_start()

    def stop(self) -> None:
        try:
            self.disconnect()
        except Exception:
            logger.exception("Error during MQTT disconnect")
        if self._is_threaded:
            self.loop_stop()

    def setup_reconnect(self, min_delay=MQTT_RECONNECT_MIN_DELAY, max_delay=MQTT_RECONNECT_MAX_DELAY):
        super().reconnect_delay_set(min_delay=min_delay, max_delay=max_delay)
