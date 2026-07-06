import asyncio
import logging
import random
import string
from urllib.parse import urlparse

import paho_socket

MQTT_KEEPALIVE = 30  # seconds
MQTT_RECONNECT_MIN_DELAY = 1  # seconds
MQTT_RECONNECT_MAX_DELAY = 120  # seconds

RETRY_DELAY = 2  # seconds
MAX_RETRIES = 5

logger = logging.getLogger(__name__)


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

    async def _connect_async(self):
        max_retries = MAX_RETRIES
        retry_delay = RETRY_DELAY
        scheme = self._broker_url.scheme

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Attempting MQTT connection to %s:%s (attempt %s)",
                    self._broker_url.hostname,
                    self._broker_url.port,
                    attempt + 1,
                )
                if scheme == "unix":
                    self.sock_connect(self._broker_url.path)
                    logger.info("MQTT connected via UNIX socket")
                elif scheme in ["mqtt-tcp", "tcp", "ws"] and self._broker_url.port:
                    logger.info("MQTT connected via %s", self._broker_url)
                    self.connect(self._broker_url.hostname, self._broker_url.port, keepalive=MQTT_KEEPALIVE)
                    logger.info("MQTT connected via %s", scheme.upper())
                else:
                    raise ValueError(f"Unknown MQTT URL scheme: {scheme}")
                logger.info("MQTT connection successful")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        "MQTT connection attempt %s failed: %s. Retrying in %ss...",
                        attempt + 1,
                        e,
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error("MQTT connection failed after %s attempts: %s", max_retries, e)
                    raise

    def start(self) -> None:
        scheme = self._broker_url.scheme

        if self._broker_url.username:
            self.username_pw_set(self._broker_url.username, self._broker_url.password)

        if scheme == "ws" and self._broker_url.path:
            self.ws_set_options(self._broker_url.path)

        self.setup_reconnect()

        if self._is_threaded:
            self.loop_start()

        asyncio.create_task(self._connect_async())

    def stop(self) -> None:
        try:
            self.disconnect()
        except Exception:
            logger.exception("Error during MQTT disconnect")
        if self._is_threaded:
            self.loop_stop()

    def setup_reconnect(self, min_delay=MQTT_RECONNECT_MIN_DELAY, max_delay=MQTT_RECONNECT_MAX_DELAY):
        super().reconnect_delay_set(min_delay=min_delay, max_delay=max_delay)
