import argparse
import asyncio
import json
import logging
import sys
from typing import Optional

from wb_welrok import config
from wb_welrok.device_config_manager import ConfigManager
from wb_welrok.wb_welrok_client import WelrokClient

logger = logging.getLogger(__name__)

# Exit codes from the WB service guideline; 2 and 6 are RestartPreventExitStatus in the unit,
# 7 is a SuccessExitStatus. argparse exits with 2 on bad arguments by itself.
EXIT_SUCCESS = 0
EXIT_NOTCONFIGURED = 6
EXIT_NOTRUNNING = 7


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
        try:
            config_file = to_json(args.config)
        except (OSError, ValueError) as exc:  # json.JSONDecodeError is a ValueError
            logger.error("Cannot read %s: %s", args.config, exc)
            return EXIT_NOTCONFIGURED
        json.dump(config_file, sys.stdout, sort_keys=True, indent=2)
        return EXIT_SUCCESS

    config_devices = ConfigManager(args.config, config.SCHEMA_FILEPATH).load_and_validate()
    if config_devices is None:
        logger.error("Invalid configuration, exiting")
        return EXIT_NOTCONFIGURED

    setup_logging(config_devices.debug)
    if not config_devices.devices:
        logger.info("No devices with an id are configured, nothing to do")
        return EXIT_NOTRUNNING

    logger.info("Welrok service starting")
    try:
        # run() installs the SIGINT/SIGTERM handlers and returns the exit code
        return asyncio.run(WelrokClient(config_devices).run())
    finally:
        logger.info("Welrok service stopped")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
