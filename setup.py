#!/usr/bin/env python

import re

from setuptools import setup


def get_version():
    """Return version string found in the package or fallback to debian/changelog.

    We prefer to read __version__ from the package so that isolated PEP 517
    builds (which create sdist without debian/) can determine a stable
    version. If the package file is missing or can't be parsed, fall back to
    parsing debian/changelog (used in some packaging workflows).
    """
    try:
        with open("wb_welrok/__init__.py", "r", encoding="utf-8") as f:
            m = re.search(r"^__version__\s*=\s*['\"]([^'\"]+)['\"]", f.read(), re.M)
            if m:
                return m.group(1)
    except OSError:
        pass

    try:
        with open("debian/changelog", "r", encoding="utf-8") as f:
            return f.readline().split()[1][1:-1]
    except OSError:
        pass

    return "0.0.0"


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
