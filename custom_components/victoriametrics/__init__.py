"""Support for sending data to a VictoriaMetrics installation."""
from contextlib import suppress
import logging
import queue
import socket
import threading
import time

import voluptuous as vol

from homeassistant.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_PREFIX,
    CONF_PROTOCOL,
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import state
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType

_LOGGER = logging.getLogger(__name__)

PROTOCOL_TCP = "tcp"
PROTOCOL_UDP = "udp"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 2003
DEFAULT_PROTOCOL = PROTOCOL_TCP
DEFAULT_PREFIX = "ha"
DOMAIN = "victoriametrics"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_HOST, default=DEFAULT_HOST): cv.string,
                vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
                vol.Optional(CONF_PROTOCOL, default=DEFAULT_PROTOCOL): vol.Any(
                    PROTOCOL_TCP, PROTOCOL_UDP
                ),
                vol.Optional(CONF_PREFIX, default=DEFAULT_PREFIX): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the VictoriaMetrics feeder."""
    conf = config[DOMAIN]
    host = conf.get(CONF_HOST)
    prefix = conf.get(CONF_PREFIX)
    port = conf.get(CONF_PORT)
    protocol = conf.get(CONF_PROTOCOL)

    if protocol == PROTOCOL_TCP:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.shutdown(2)
            _LOGGER.debug("Connection to VictoriaMetrics possible")
        except OSError:
            _LOGGER.error("Not able to connect to VictoriaMetrics")
            return False
    else:
        _LOGGER.debug("No connection check for UDP possible")

    VictoriaMetricsFeeder(hass, host, port, protocol, prefix)
    return True


class VictoriaMetricsFeeder(threading.Thread):
    """Feed data to VictoriaMetrics using Graphite protocol."""

    def __init__(self, hass, host, port, protocol, prefix):
        """Initialize the feeder."""
        super().__init__(daemon=True)
        self._hass = hass
        self._host = host
        self._port = port
        self._protocol = protocol
        # rstrip any trailing dots in case they think they need it
        self._prefix = prefix.rstrip(".")
        self._queue = queue.Queue()
        self._quit_object = object()
        self._we_started = False

        hass.bus.listen_once(EVENT_HOMEASSISTANT_START, self.start_listen)
        hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, self.shutdown)
        hass.bus.listen(EVENT_STATE_CHANGED, self.event_listener)
        _LOGGER.debug("VictoriaMetrics feeding to %s:%i initialized", self._host, self._port)

    def start_listen(self, event):
        """Start event-processing thread."""
        _LOGGER.debug("Event processing thread started")
        self._we_started = True
        self.start()

    def shutdown(self, event):
        """Signal shutdown of processing event."""
        _LOGGER.debug("Event processing signaled exit")
        self._queue.put(self._quit_object)

    def event_listener(self, event):
        """Queue an event for processing."""
        if self.is_alive() or not self._we_started:
            _LOGGER.debug("Received event")
            self._queue.put(event)
        else:
            _LOGGER.error("VictoriaMetrics feeder thread has died, not queuing event")

    def _send_to_victoriametrics(self, data):
        """Send data to VictoriaMetrics using Graphite protocol."""
        if self._protocol == PROTOCOL_TCP:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self._host, self._port))
            sock.sendall(data.encode("utf-8"))
            sock.send(b"\n")
            sock.close()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(data.encode("utf-8") + b"\n", (self._host, self._port))

    def _report_attributes(self, entity_id, new_state):
        """Report the attributes."""
        now = time.time()
        things = dict(new_state.attributes)
        _LOGGER.debug('attributes: %s', things)
        with suppress(ValueError):
            things["state"] = state.state_as_number(new_state)

        key_values = []
        tags = []
        for key, value in things.items():
            if isinstance(value, (float, int)):
                key_values.append((key, value))
            else:
                tags.append(f'{key}={value}'.replace(';', '_').replace(' ', '_'))

        if tags:
            tag = ';' + ';'.join(tags)
        else:
            tag = ''

        if not key_values:
            # If there is no numeric state, use 0 so we at least post attributes
            key_values.append(('state', 0))

        lines = [
            "%s.%s.%s%s %f %i"
            % (self._prefix, entity_id, key.replace(" ", "_"), tag, value, now)
            for key, value in key_values
        ]
        if not lines:
            return

        _LOGGER.debug("Sending to victoriametrics:\n%s\n\t", '\n\t'.join(lines))
        try:
            self._send_to_victoriametrics("\n".join(lines))
        except socket.gaierror:
            _LOGGER.error("Unable to connect to host %s", self._host)
        except OSError:
            _LOGGER.exception("Failed to send data to victoriametrics")

    def run(self):
        """Run the process to export the data."""
        while True:
            if (event := self._queue.get()) == self._quit_object:
                _LOGGER.debug("Event processing thread stopped")
                self._queue.task_done()
                return
            if event.event_type == EVENT_STATE_CHANGED:
                if not event.data.get("new_state"):
                    _LOGGER.debug(
                        "Skipping %s without new_state for %s",
                        event.event_type,
                        event.data["entity_id"],
                    )
                    self._queue.task_done()
                    continue

                _LOGGER.debug(
                    "Processing STATE_CHANGED event for %s", event.data["entity_id"]
                )
                try:
                    self._report_attributes(
                        event.data["entity_id"], event.data["new_state"]
                    )
                except Exception:  # pylint: disable=broad-except
                    # Catch this so we can avoid the thread dying and
                    # make it visible.
                    _LOGGER.exception("Failed to process STATE_CHANGED event")
            else:
                _LOGGER.warning("Processing unexpected event type %s", event.event_type)

            self._queue.task_done()
