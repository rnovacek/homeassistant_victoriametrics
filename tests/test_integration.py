"""Lifecycle checks with a small HA harness and real conversion dependencies."""
import asyncio
from datetime import date, datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from test_delivery import Session


@pytest.fixture
def integration(monkeypatch):
    modules = {}
    for name in ['homeassistant', 'homeassistant.const', 'homeassistant.core', 'homeassistant.helpers',
                 'homeassistant.helpers.state', 'homeassistant.helpers.aiohttp_client',
                 'homeassistant.helpers.config_validation']:
        modules[name] = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, modules[name])
    const = modules['homeassistant.const']
    for key, value in dict(CONF_URL='url', CONF_PREFIX='prefix', EVENT_HOMEASSISTANT_STOP='stop', EVENT_STATE_CHANGED='state_changed').items():
        setattr(const, key, value)
    core = modules['homeassistant.core']
    core.Event = core.State = SimpleNamespace
    core.callback = lambda f: f
    modules['homeassistant.helpers.state'].state_as_number = lambda state: float(state.state)
    modules['homeassistant.helpers.aiohttp_client'].async_get_clientsession = lambda hass: hass.session
    cv = modules['homeassistant.helpers.config_validation']
    cv.string = str
    cv.positive_int = int
    path = Path(__file__).resolve().parents[1] / 'custom_components' / 'victoriametrics' / '__init__.py'
    spec = importlib.util.spec_from_file_location('vm_test_integration', path, submodule_search_locations=[str(path.parent)])
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def event(entity='sensor.temperature', **attributes):
    return SimpleNamespace(data={'entity_id': entity, 'new_state': SimpleNamespace(state='0', attributes=attributes)},
                           time_fired=datetime.now(timezone.utc))


def test_attribute_conversion_regression(integration):
    value = event(day=date(2026, 9, 15), text='Kitchen', timestamp='2026-09-15T00:00:00+00:00', enabled=False)
    records = {r['metric']['__name__']: r for r in map(json.loads, integration.event_metrics(value, 'ha'))}
    assert records['ha.sensor.temperature.value']['values'] == [0]
    assert records['ha.sensor.temperature.enabled']['values'] == [0]
    assert records['ha.sensor.temperature.timestamp']['values'] == [1789430400000]
    assert records['ha.sensor.temperature.value']['metric']['day'] == '2026-09-15'


class Hass:
    def __init__(self, path):
        self.data = {}
        self.config = SimpleNamespace(path=lambda *args: str(path.joinpath(*args)))
        self.session = Session([401])
        self.listeners = {}
        self.states = SimpleNamespace(async_set=self.set_state)
        self.bus = SimpleNamespace(async_listen=self.listen, async_listen_once=self.listen)
        self.published = []

    def set_state(self, entity, state, attributes):
        self.published.append((entity, state, attributes))
        if 'state_changed' in self.listeners:
            self.listeners['state_changed'](event(entity))

    def listen(self, name, fn):
        self.listeners[name] = fn
        return lambda: self.listeners.pop(name, None)

    async def async_add_executor_job(self, fn, *args):
        return await asyncio.to_thread(fn, *args)

    def async_create_task(self, coroutine, name):
        return asyncio.create_task(coroutine, name=name)

    async_create_background_task = async_create_task


def test_offline_startup_persistence_shutdown_and_restart(integration, tmp_path):
    async def run():
        config = integration.CONFIG_SCHEMA({'victoriametrics': {}})
        hass = Hass(tmp_path)
        assert await integration.async_setup(hass, config)
        hass.listeners['state_changed'](event())
        worker = hass.data['victoriametrics']
        while worker.status != 'authentication_error':
            await asyncio.sleep(0.005)
        assert worker.buffer.stats()['pending_records'] == 1
        # Shutdown must interrupt the five-minute retry and persist accepted events.
        hass.listeners['state_changed'](event())
        await asyncio.wait_for(hass.listeners['stop'](None), 2)
        assert 'state_changed' not in hass.listeners
        recovered = integration.MetricBuffer(tmp_path / '.storage/victoriametrics.sqlite3', 100, 10000)
        assert recovered.stats()['pending_records'] == 2
        recovered.close()
        # Status updates did not enter the buffer, even though they fired state events.
        assert hass.published[-1][2]['pending_records'] == 2
        second = Hass(tmp_path)
        second.session = Session([204])
        assert await integration.async_setup(second, config)
        while second.data['victoriametrics'].last_success is None:
            await asyncio.sleep(0.005)
        await asyncio.wait_for(second.listeners['stop'](None), 2)
        assert second.published[-1][2]['pending_records'] == 0
    asyncio.run(asyncio.wait_for(run(), 5))
