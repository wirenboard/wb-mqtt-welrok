# Wiren Board MQTT Driver for Welrok thermostat Welrok az

[Wiki Official link](https://wiki.wirenboard.com/wiki/Welrok_az)

[Welrok site](https://welrok.com/)

The driver is packaged as a regular Debian package: its Python dependencies (`python3-aiohttp`,
`python3-jsonschema`, `python3-paho-mqtt` >= 2.1) come from Debian.

By default it connects to the local broker through `unix:///var/run/mosquitto/mosquitto.sock`;
set `mqtt_server_uri` in `/etc/wb-welrok.conf` to use another broker, e.g.
`tcp://user:password@host:1883`.

## Service lifecycle

Exit codes follow the Wiren Board service guideline: `0` on SIGINT/SIGTERM (the WB devices are
removed from the broker first, an error is logged when the broker is not connected), `2` on bad
arguments or when the broker rejects the MQTT login, `6` when the config cannot be read or is
invalid (including the broker URL), `7` when no device with an id is configured. An unavailable
broker is waited for; after a reconnect the devices and their command subscriptions are republished.
