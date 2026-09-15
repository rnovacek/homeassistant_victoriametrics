"""Record Home Assistant state changes in VictoriaMetrics with durable delivery."""
import asyncio
from contextlib import suppress
from datetime import datetime
import json
import logging
from typing import Dict, List, Tuple, TypedDict, Union

import ciso8601
import voluptuous as vol

from homeassistant.const import CONF_URL, CONF_PREFIX, EVENT_HOMEASSISTANT_STOP, EVENT_STATE_CHANGED
from homeassistant.core import Event, State, callback
from homeassistant.helpers import state
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv

from .buffer import MetricBuffer
from .delivery import Delivery

_LOGGER = logging.getLogger(__name__)
DOMAIN = "victoriametrics"
STATUS_ENTITY = "sensor.victoriametrics_delivery"

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Optional(CONF_URL, default="http://localhost:8428"): cv.string,
        vol.Optional(CONF_PREFIX, default="ha"): cv.string,
        vol.Optional("max_queue_records", default=100000): cv.positive_int,
        vol.Optional("max_queue_bytes", default=64 * 1024 * 1024): cv.positive_int,
        vol.Optional("batch_size", default=500): vol.All(cv.positive_int, vol.Range(min=1, max=10000)),
        vol.Optional("flush_interval", default=1): vol.All(vol.Coerce(float), vol.Range(min=0.1)),
    }),
}, extra=vol.ALLOW_EXTRA)


async def async_setup(hass, config):
    """Start buffering immediately; no network access is required for setup."""
    conf = config[DOMAIN]
    try:
        buffer = await hass.async_add_executor_job(
            MetricBuffer, hass.config.path('.storage', 'victoriametrics.sqlite3'),
            conf['max_queue_records'], conf['max_queue_bytes'],
        )
    except Exception:
        _LOGGER.exception('Cannot open VictoriaMetrics persistent queue')
        return False

    @callback
    def publish(status, attributes):
        hass.states.async_set(STATUS_ENTITY, status, {
            'friendly_name': 'VictoriaMetrics delivery', **attributes,
        })

    delivery = Delivery(buffer, async_get_clientsession(hass), conf[CONF_URL],
                        hass.async_add_executor_job, publish,
                        conf['batch_size'], conf['flush_interval'])
    pending = set()
    accepting = True
    last_drop_log = 0
    last_persist_error_log = 0

    async def persist(event):
        nonlocal last_drop_log, last_persist_error_log
        try:
            metrics = event_metrics(event, conf[CONF_PREFIX].rstrip('.'))
            dropped = await hass.async_add_executor_job(buffer.append, metrics, event.time_fired.timestamp())
            delivery.ingestion_error = None
            if dropped:
                now = asyncio.get_running_loop().time()
                if not last_drop_log or now - last_drop_log >= 300:
                    _LOGGER.error('VictoriaMetrics queue limit reached; oldest records dropped (see delivery sensor)')
                    last_drop_log = now
        except Exception:
            delivery.lost_events += 1
            delivery.ingestion_error = 'Cannot convert or persist event'
            now = asyncio.get_running_loop().time()
            if not last_persist_error_log or now - last_persist_error_log >= 300:
                _LOGGER.exception('Cannot persist VictoriaMetrics event; this event was lost')
                last_persist_error_log = now
            publish('storage_error', {'last_error': delivery.ingestion_error, 'lost_events': delivery.lost_events})

    @callback
    def receive(event):
        if not accepting or not event.data.get('new_state') or event.data.get('entity_id') == STATUS_ENTITY:
            return
        task = hass.async_create_task(persist(event), 'victoriametrics persist')
        pending.add(task)
        task.add_done_callback(pending.discard)

    unsubscribe = hass.bus.async_listen(EVENT_STATE_CHANGED, receive)
    worker = hass.async_create_background_task(delivery.run(), 'victoriametrics delivery')

    async def shutdown(event):
        nonlocal accepting
        accepting = False
        unsubscribe()
        delivery.stopping.set()
        # Complete already accepted disk writes before closing the queue.
        if pending:
            await asyncio.gather(*list(pending))
        try:
            await asyncio.wait_for(asyncio.shield(worker), timeout=10)
        except asyncio.TimeoutError:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker
        await delivery.report()
        await hass.async_add_executor_job(buffer.close)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, shutdown)
    hass.data[DOMAIN] = delivery
    return True


class Metric(TypedDict):
    metric: Dict[str, str]
    values: List[float]
    timestamps: List[float]


def event_metrics(event: Event, prefix: str):
    """Convert a state change to JSON lines, preserving its original timestamp."""
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
        elif isinstance(value, str):
            try:
                num_value = ciso8601.parse_datetime(value).timestamp() * 1000
            except ValueError:
                num_value = None

        if num_value is not None:
            key_values.append((key, num_value))
        else:
            tags.append((key, str(value)))

    if not key_values:
        # If there is no numeric state, use 0 so we at least post attributes
        key_values.append(('value', 0))
        tags.append(('value', new_state.state))

    metrics: List[str] = []

    for key, value in key_values:
        metric_name = f'{prefix}.{entity_id}.{key.replace(" ", "_")}'

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

    return metrics
