"""Support for sending data to a VictoriaMetrics installation."""
from contextlib import suppress
from datetime import datetime
import json
import logging
import queue
import threading
from typing import Dict, List, Sequence, Tuple, TypedDict, Union

import voluptuous as vol
import requests
import ciso8601

from homeassistant.const import (
    CONF_URL,
    CONF_PREFIX,
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
)
from homeassistant.core import HomeAssistant, Event, State
from homeassistant.helpers import state
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType

_LOGGER = logging.getLogger(__name__)

DEFAULT_URL = "http://localhost:8428"
DEFAULT_PREFIX = "ha"
DOMAIN = "victoriametrics"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_URL, default=DEFAULT_URL): cv.string,
                vol.Optional(CONF_PREFIX, default=DEFAULT_PREFIX): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the VictoriaMetrics feeder."""
    conf = config[DOMAIN]
    url = conf.get(CONF_URL)
    prefix = conf.get(CONF_PREFIX)


    response = requests.get(url, timeout=2)
    if not response.ok:
        _LOGGER.error("Not able to connect to VictoriaMetrics")
    else:
        _LOGGER.debug("Connection to VictoriaMetrics possible")

    VictoriaMetricsFeeder(hass, url, prefix)
    return True


class Metric(TypedDict):
    metric: Dict[str, str]
    values: List[float]
    timestamps: List[float]


class VictoriaMetricsFeeder(threading.Thread):
    """Feed data to VictoriaMetrics using Graphite protocol."""

    def __init__(self, hass: HomeAssistant, url: str, prefix: str):
        """Initialize the feeder."""
        super().__init__(daemon=True)
        self._hass = hass
        self._url = url
        # rstrip any trailing dots in case they think they need it
        self._prefix = prefix.rstrip(".")
        self._queue = queue.Queue()
        self._quit_object = object()
        self._we_started = False

        hass.bus.listen_once(EVENT_HOMEASSISTANT_START, self.start_listen)
        hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, self.shutdown)
        hass.bus.listen(EVENT_STATE_CHANGED, self.event_listener)
        _LOGGER.debug("VictoriaMetrics feeding to %s initialized", self._url)

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

    def _send_to_victoriametrics(self, metrics: Sequence[str]):
        """Send data to VictoriaMetrics using Graphite protocol."""
        response = requests.post(f'{self._url}/api/v1/import', data='\n'.join(metrics).encode('utf-8'), timeout=5)
        if response.ok:
            _LOGGER.debug('%d metrics successfully sent to victoriametrics', len(metrics))
        else:
            _LOGGER.error('Unable to send metrics to victoriametrics: %d %s', response.status_code, response.content)

    def _report_event(self, event: Event):
        #entity_id, new_state
        """Report the event."""
        entity_id = event.data['entity_id']
        new_state: State = event.data['new_state']

        things = dict(new_state.attributes)

        with suppress(ValueError):
            things['value'] = state.state_as_number(new_state)

        key_values: List[Tuple[str, Union[int, float]]] = []
        tags = []
        for key, value in things.items():
            num_value = None
            if value is None:
                continue
            elif isinstance(value, list):
                continue
            elif isinstance(value, dict):
                continue
            elif isinstance(value, tuple):
                continue
            elif isinstance(value, datetime):
                num_value = value.timestamp()
            elif isinstance(value, bool):
                num_value = int(value)
            elif isinstance(value, (float, int)):
                num_value = value
            else:
                try:
                    num_value = ciso8601.parse_datetime(value).timestamp() * 1000
                except ValueError:
                    num_value = None

            if num_value is not None:
                key_values.append((key, num_value))
            else:
                tags.append((key, value))

        if not key_values:
            # If there is no numeric state, use 0 so we at least post attributes
            key_values.append(('value', 0))
            tags.append(('value', new_state.state))

        metrics: List[str] = []

        for key, value in key_values:
            metric_name = f'{self._prefix}.{entity_id}.{key.replace(" ", "_")}'

            metric: Metric = {
                'metric': {
                    '__name__': metric_name,
                },
                'values': [value],
                'timestamps': [int(event.time_fired.timestamp() * 1000)],
            }
            for tag_key, tag_value in tags:
                metric['metric'][tag_key] = tag_value

            metrics.append(json.dumps(metric, indent=None))

        if not metrics:
            return

        _LOGGER.debug("Sending to victoriametrics:\n%s\n\t", '\n\t'.join(metrics))
        self._send_to_victoriametrics(metrics)

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
                    self._report_event(event)
                except Exception:  # pylint: disable=broad-except
                    # Catch this so we can avoid the thread dying and
                    # make it visible.
                    _LOGGER.exception("Failed to process STATE_CHANGED event")
            else:
                _LOGGER.warning("Processing unexpected event type %s", event.event_type)

            self._queue.task_done()
