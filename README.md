# Wiren Board MQTT Driver for Welrok thermostat Welrok az

[Wiki Official link](https://wiki.wirenboard.com/wiki/Welrok_az)

[Welrok site](https://welrok.com/)

This is a distribution of "wb-welrok" as a self-contained
    Python virtualenv wrapped into a Debian package ("omnibus" package,
    all passengers on board). The packaged virtualenv is kept in sync with
    the host's interpreter automatically.

## Service lifecycle

Exit codes follow the Wiren Board service guideline: `0` on SIGINT/SIGTERM (the WB devices are
removed from the broker first, an error is logged when the broker is not connected), `2` on bad
arguments or when the broker rejects the MQTT login, `6` when the config cannot be read or is
invalid (including the broker URL), `7` when no device with an id is configured. An unavailable
broker is waited for; after a reconnect the devices and their command subscriptions are republished.
