"""Asynchronous delivery of durable batches."""
import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
import random
import time

import aiohttp

_LOGGER = logging.getLogger(__name__)


def retry_after(value):
    """Accept either Retry-After seconds or an HTTP date."""
    if not value:
        return 0
    try:
        return max(0, float(value))
    except ValueError:
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 0


class Delivery:
    def __init__(self, buffer, session, url, execute, publish, batch_size=500, flush_interval=1):
        self.buffer = buffer
        self.session = session
        self.url = url.rstrip('/') + '/api/v1/import'
        self.execute = execute
        self.publish = publish
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.stopping = asyncio.Event()
        self.status = 'starting'
        self.last_error = None
        self.last_success = None
        self.failed_attempts = 0
        self.consecutive_failures = 0
        self.last_log = 0
        self.next_retry = None
        self.ingestion_error = None
        self.lost_events = 0

    async def report(self):
        stats = await self.execute(self.buffer.stats)
        oldest = stats.pop('oldest_timestamp')
        stats.update(
            oldest_record_age_seconds=max(0, int(time.time() - oldest)) if oldest is not None else 0,
            last_successful_write=self.last_success,
            failed_attempts=self.failed_attempts,
            consecutive_failures=self.consecutive_failures,
            last_error=self.last_error,
            next_retry=self.next_retry,
            lost_events=self.lost_events,
        )
        if self.ingestion_error:
            stats['last_error'] = self.ingestion_error
        self.publish('storage_error' if self.ingestion_error else self.status, stats)

    def failure(self, status, error):
        self.failed_attempts += 1
        self.consecutive_failures += 1
        now = time.monotonic()
        if self.status != status or now - self.last_log >= 300:
            _LOGGER.warning('VictoriaMetrics: %s; pending data retained', error)
            self.last_log = now
        self.status = status
        self.last_error = error

    async def send(self, rows):
        """Return retry delay, or None when this batch has been handled."""
        if self.stopping.is_set():
            return None
        try:
            async with self.session.post(
                self.url, data=('\n'.join(row[1] for row in rows) + '\n').encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=5), allow_redirects=False,
            ) as response:
                code = response.status
                delay = retry_after(response.headers.get('Retry-After'))
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self.failure('unavailable', type(exc).__name__)
            delay = 0
        else:
            if 200 <= code < 300:
                await self.execute(self.buffer.remove, rows)
                if self.status in ('unavailable', 'authentication_error', 'http_error'):
                    _LOGGER.info('VictoriaMetrics connection recovered; replaying buffered data')
                self.status = 'connected'
                self.last_error = None
                self.last_success = datetime.now(timezone.utc).isoformat()
                self.consecutive_failures = 0
                return None
            if code in (400, 413):
                self.failed_attempts += 1
                # Split rejected batches to keep a single bad record from blocking delivery.
                if len(rows) > 1:
                    middle = len(rows) // 2
                    first_delay = await self.send(rows[:middle])
                    if first_delay is not None:
                        return first_delay
                    return await self.send(rows[middle:])
                await self.execute(self.buffer.remove, rows, True)
                if time.monotonic() - self.last_log >= 300 or self.last_error != 'Rejected metric':
                    _LOGGER.error('VictoriaMetrics rejected a single metric (HTTP %s); dropped it', code)
                    self.last_log = time.monotonic()
                self.last_error = 'Rejected metric'
                return None
            if code in (401, 403):
                self.failure('authentication_error', f'HTTP {code}: check server authentication')
                return max(300, delay)
            self.failure('unavailable' if code == 429 or code >= 500 else 'http_error', f'HTTP {code}')
            if code not in (408, 429) and code < 500:
                return max(300, delay)
        backoff = min(60, 2 ** min(self.consecutive_failures - 1, 6))
        return max(delay, random.uniform(backoff / 2, backoff))

    async def wait(self, seconds):
        """Refresh diagnostics during long retries, with interruptible shutdown."""
        deadline = time.monotonic() + seconds
        while not self.stopping.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(self.stopping.wait(), min(30, remaining))
            except asyncio.TimeoutError:
                await self.report()

    async def run(self):
        try:
            await self.report()
            while not self.stopping.is_set():
                rows = await self.execute(self.buffer.batch, self.batch_size)
                if not rows:
                    await self.wait(self.flush_interval)
                    continue
                delay = await self.send(rows)
                self.next_retry = datetime.fromtimestamp(time.time() + delay, timezone.utc).isoformat() if delay is not None else None
                await self.report()
                if delay is not None:
                    await self.wait(delay)
                else:
                    await self.wait(self.flush_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.status = 'storage_error'
            _LOGGER.exception('VictoriaMetrics delivery stopped unexpectedly; persisted records retained')
            try:
                await self.report()
            except Exception:
                self.publish(self.status, {'last_error': 'Cannot access persistent queue'})
