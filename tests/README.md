# Tests

Run from the repository root:

```sh
python3 -m pytest tests/
```

The tests use mocks and temporary files; no MQTT broker, controller, network access, or Welrok
thermostat is required.

`test_mqtt_client.py` checks broker URL validation, nonblocking threaded startup, credentials, and
shutdown ordering. `test_main.py` checks service exit codes, configuration failures, MQTT
authentication handling, state restoration after reconnect, and cleanup of an unavailable device.

| Test | Verifies |
| --- | --- |
| `test_client_id_is_unique` | MQTT client identifiers do not collide |
| `test_invalid_broker_url_is_rejected` | incomplete and unsupported broker URIs are invalid |
| `test_threaded_tcp_client_starts_nonblocking_connection` | TCP startup leaves the main loop responsive |
| `test_threaded_unix_client_starts_nonblocking_connection` | UNIX socket startup leaves the main loop responsive |
| `test_credentials_are_applied_before_connection` | URI credentials reach Paho before connecting |
| `test_stop_disconnects_before_stopping_network_loop` | queued cleanup messages can be sent before shutdown |
| `test_empty_configuration_exits_with_status_7` | a placeholder device is treated as no work |
| `test_unreadable_configuration_exits_with_status_6` | filesystem errors are configuration errors |
| `test_invalid_broker_is_rejected_by_config_manager` | a bad broker URI produces status 6 at startup |
| `test_device_republishes_metadata_values_and_subscriptions` | reconnect restores retained state and commands |
| `test_root_mqtt_reconnect_republishes_active_devices` | root reconnect reaches every active thermostat |
| `test_authentication_refusal_exits_with_status_2` | CONNACK authentication refusal produces status 2 |
| `test_removing_unavailable_device_closes_session` | shutdown cleans a thermostat which never answered |
