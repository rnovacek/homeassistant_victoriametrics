"""Exercise the real SQLite queue and HTTP sender without a Home Assistant install."""
import asyncio
import importlib.util
import json
from pathlib import Path
import time
from types import SimpleNamespace

import aiohttp
from aiohttp import web
import pytest

ROOT = Path(__file__).resolve().parents[1] / 'custom_components' / 'victoriametrics'


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MetricBuffer = load('buffer').MetricBuffer
delivery_module = load('delivery')
Delivery = delivery_module.Delivery


@pytest.fixture
def buffer(tmp_path):
    result = MetricBuffer(tmp_path / 'queue.sqlite3', 100, 10000)
    yield result
    result.close()


def test_restart_retains_exact_payload_and_drops(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    queue = MetricBuffer(path, 2, 100)
    assert queue.append(['old', 'middle', 'new'], 123) == 1
    queue.close()
    queue = MetricBuffer(path, 2, 100)
    assert [p for _, p in queue.batch(10)] == ['middle', 'new']
    assert queue.stats() == dict(pending_records=2, pending_bytes=9, oldest_timestamp=123, dropped_records=1)
    queue.remove(queue.batch(1))
    assert queue.stats()['pending_bytes'] == 3
    queue.close()


def test_byte_limit_and_evicted_inflight_ack(tmp_path):
    queue = MetricBuffer(tmp_path / 'queue.sqlite3', 100, 4)
    queue.append(['é'], 1)
    inflight = queue.batch(10)
    assert queue.append(['abcd'], 2) == 1
    queue.remove(inflight, True)
    assert queue.stats()['dropped_records'] == 1
    assert queue.stats()['pending_bytes'] == 4
    queue.close()


class Session:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.payloads = []

    def post(self, url, **kwargs):
        self.payloads.append(kwargs['data'])
        outcome = self.outcomes.pop(0)

        class Response:
            async def __aenter__(self):
                if isinstance(outcome, Exception):
                    raise outcome
                if isinstance(outcome, tuple):
                    return SimpleNamespace(status=outcome[0], headers={'Retry-After': outcome[1]})
                return SimpleNamespace(status=outcome, headers={})

            async def __aexit__(self, *args):
                pass
        return Response()


async def execute(fn, *args):
    return await asyncio.to_thread(fn, *args)


def sender(buffer, session):
    return Delivery(buffer, session, 'http://example.test', execute, lambda *args: None)


@pytest.mark.parametrize('outcome', [aiohttp.ClientConnectionError(), asyncio.TimeoutError(), 429, 500, 503])
def test_transient_failure_retries_identical_payload(buffer, outcome):
    async def run():
        payload = json.dumps({'metric': {'__name__': 'ha.test'}, 'values': [0], 'timestamps': [1234567]})
        buffer.append([payload], time.time())
        session = Session([outcome, 204])
        worker = sender(buffer, session)
        assert await worker.send(buffer.batch(10)) > 0
        assert buffer.stats()['pending_records'] == 1
        assert await worker.send(buffer.batch(10)) is None
        assert buffer.stats()['pending_records'] == 0
        assert session.payloads[0] == session.payloads[1]
        assert worker.failed_attempts == 1
        assert worker.status == 'connected'
    asyncio.run(run())


@pytest.mark.parametrize('status', [401, 403, 404, 302])
def test_permanent_error_keeps_data_and_retries_slowly(buffer, status):
    async def run():
        buffer.append(['metric'], 1)
        worker = sender(buffer, Session([status]))
        assert await worker.send(buffer.batch(10)) >= 300
        assert buffer.stats()['pending_records'] == 1
        assert worker.status == ('authentication_error' if status in (401, 403) else 'http_error')
    asyncio.run(run())


def test_retry_after(buffer):
    async def run():
        buffer.append(['metric'], 1)
        worker = sender(buffer, Session([(429, '120')]))
        assert await worker.send(buffer.batch(10)) == 120
    asyncio.run(run())
    assert delivery_module.retry_after('invalid') == 0
    assert delivery_module.retry_after('Wed, 21 Oct 2099 07:28:00 GMT') > 120


def test_bad_record_isolation(buffer):
    async def run():
        buffer.append(['bad', 'good'], 1)
        session = Session([400, 400, 204])
        worker = sender(buffer, session)
        assert await worker.send(buffer.batch(10)) is None
        assert buffer.stats()['pending_records'] == 0
        assert buffer.stats()['dropped_records'] == 1
        assert session.payloads == [b'bad\ngood\n', b'bad\n', b'good\n']
    asyncio.run(run())


def test_split_failure_retains_unsent_records(buffer):
    async def run():
        buffer.append(['one', 'two'], 1)
        worker = sender(buffer, Session([413, 503]))
        assert await worker.send(buffer.batch(10)) > 0
        assert buffer.stats()['pending_records'] == 2
    asyncio.run(run())


def test_shutdown_interrupts_backoff_and_publishes_status(buffer):
    async def run():
        buffer.append(['metric'], time.time() - 60)
        reports = []
        worker = Delivery(buffer, Session([401]), 'http://example.test', execute, lambda *args: reports.append(args))
        task = asyncio.create_task(worker.run())
        while worker.status != 'authentication_error':
            await asyncio.sleep(0.001)
        worker.stopping.set()
        await asyncio.wait_for(task, 1)
        assert buffer.stats()['pending_records'] == 1
        assert reports[-1][1]['oldest_record_age_seconds'] >= 60
        assert reports[-1][1]['next_retry'] is not None
    asyncio.run(run())


def test_real_http_batches_replay(buffer):
    async def run():
        received = []
        async def handler(request):
            received.append(await request.text())
            return web.Response(status=503 if len(received) == 1 else 204)
        app = web.Application()
        app.router.add_post('/api/v1/import', handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as session:
                buffer.append(['one', 'two'], time.time())
                worker = Delivery(buffer, session, f'http://127.0.0.1:{port}', execute, lambda *args: None)
                assert await worker.send(buffer.batch(10)) > 0
                await worker.send(buffer.batch(10))
                assert received == ['one\ntwo\n', 'one\ntwo\n']
                assert buffer.stats()['pending_records'] == 0
        finally:
            await runner.cleanup()
    asyncio.run(run())
