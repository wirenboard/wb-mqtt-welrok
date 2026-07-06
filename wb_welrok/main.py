import argparse
import asyncio
import json
import logging
import signal
import sys
import traceback
from typing import Optional

import jsonschema

from wb_welrok import config
from wb_welrok.device_config_manager import ConfigManager
from wb_welrok.wb_welrok_client import WelrokClient

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s (%(filename)s:%(lineno)d)")
    logger.setLevel(level)


def to_json(config_filepath: str) -> dict:
    with open(config_filepath, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
        return config


def main(argv: Optional[list[str]] = None) -> int:
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j", action="store_true", help=f"Make JSON for wb-mqtt-confed from {config.CONFIG_FILEPATH}"
    )
    parser.add_argument("-c", "--config", type=str, default=config.CONFIG_FILEPATH, help="Config file")
    args = parser.parse_args(argv[1:])

    if args.j:
        config_file = to_json(args.config)
        json.dump(config_file, sys.stdout, sort_keys=True, indent=2)
        return 0

    config_devices = ConfigManager(args.config, config.SCHEMA_FILEPATH).load_and_validate()
    if config_devices is None:
        logger.error("Invalid configuration, exiting")
        return 6

    setup_logging(config_devices.debug)
    logger.info("Welrok service starting")

    welrok_client = WelrokClient(config_devices)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown():
        logger.info("Received stop signal, shutting down")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    try:
        result = loop.run_until_complete(welrok_client.run())
    except asyncio.CancelledError:
        logger.info("Shutdown complete")
        result = 0
    finally:
        loop.close()
        logger.info("Welrok service stopped")

    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
