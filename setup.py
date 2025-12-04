#!/usr/bin/env python

from setuptools import setup

setup(
    name="wb-welrok",
    use_scm_version={
        "fallback_version": "0.0.16",
    },
    setup_requires=["setuptools_scm"],
    author="Ivan Belokrylov",
    author_email="belokrylov.i@welrok.com",
    maintainer="Welrok",
    maintainer_email="info@welrok.com",
    description="Wiren Board MQTT Driver for Welrok thermostat",
    packages=["wb_welrok"],
    entry_points={"console_scripts": ["wb_welrok = wb_welrok.main:main"]},
    license="MIT",
    install_requires=[
        "rpds-py==0.5.3",
        "aiohttp==3.7.4",
        "aiosignal",
        "attrs",
        "bidict",
        "certifi",
        "charset-normalizer",
        "frozenlist",
        "h11",
        "idna",
        "multidict",
        "paho-mqtt==1.5.1",
        "paho-socket==0.0.3",
        "python-engineio",
        "referencing",
        "requests",
        "rpds-py",
        "simple-websocket",
        "urllib3",
        "wsproto",
        "yarl",
        "jsonschema",
    ],
)
