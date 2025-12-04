import argparse
import asyncio
import json
import logging
import signal
import sys
import traceback

import aiohttp
import jsonschema

from wb_welrok import config, wbmqtt
from wb_welrok.wb_welrok_client import WelrokClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s (%(filename)s:%(lineno)d)")
logger.setLevel(logging.INFO)


def read_and_validate_config(config_filepath: str, schema_filepath: str) -> dict:
    with open(config_filepath, "r", encoding="utf-8") as config_file, open(
        schema_filepath, "r", encoding="utf-8"
    ) as schema_file:
        try:
            config = json.load(config_file)
            schema = json.load(schema_file)
            jsonschema.validate(config, schema)

            id_list = [device["device_id"] for device in config["devices"]]
            if len(id_list) != len(set(id_list)):
                raise ValueError("Device ID must be unique")

            return config
        except (
            jsonschema.exceptions.ValidationError,
            ValueError,
            DeprecationWarning,
        ) as e:
            logger.error("Config file validation failed! Error: %s", e)
            return None


def to_json(config_filepath: str) -> dict:
    with open(config_filepath, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
        return config


def main(argv=sys.argv):
    logger.info("Welrok service starting")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        action="store_true",
        help=f"Make JSON for wb-mqtt-confed from {config.CONFIG_FILEPATH}",
    )
    parser.add_argument("-c", "--config", type=str, default=config.CONFIG_FILEPATH, help="Config file")
    args = parser.parse_args(argv[1:])

    if args.j:
        config_file = to_json(args.config)
        json.dump(config_file, sys.stdout, sort_keys=True, indent=2)
        return 0

    config_file = read_and_validate_config(args.config, config.SCHEMA_FILEPATH)
    if config_file is None:
        return 6
    if config_file["debug"]:
        logging.basicConfig(level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    devices = config_file["devices"]

    welrok_client = WelrokClient(devices)
    result = asyncio.run(welrok_client.run())
    logger.info("Welrok service stopped")
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
