#!/usr/bin/env python

import re

from setuptools import setup


def get_version():
    """Return version string found in the package or fallback to debian/changelog"""

    with open("debian/changelog", "r", encoding="utf-8") as f:
        return f.readline().split()[1][1:-1]


setup(
    name="wb-welrok",
    version=get_version(),
    author="Ivan Belokrylov",
    author_email="belokrylov.i@welrok.com",
    maintainer="Welrok",
    maintainer_email="info@welrok.com",
    description="Wiren Board MQTT Driver for Welrok thermostat",
    packages=["wb_welrok"],
    entry_points={"console_scripts": ["wb_welrok = wb_welrok.main:main"]},
    license="MIT",
    install_requires=["aiohttp==3.7.4", "paho-mqtt==1.5.1", "paho-socket==0.0.3", "jsonschema"],
)
