#!/usr/bin/env python

from setuptools import setup


def get_version():
    with open("debian/changelog", "r", encoding="utf-8") as f:
        return f.readline().split()[1][1:-1].split("~")[0]


setup(
    name="wb-welrok",
    version=get_version(),
    author="Ivan Belokrylov",
    author_email="belokrylov.i@welrok.com",
    maintainer="Welrok",
    maintainer_email="info@welrok.com",
    description="Wiren Board MQTT Driver for Welrok thermostat",
    packages=["wb_welrok"],
    license="MIT",
    install_requires=[
        "aiohttp",
        "jsonschema",
        "paho-mqtt>=2.1.0",
    ],
)
