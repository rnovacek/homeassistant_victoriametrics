import asyncio
import csv
from typing import Dict, Generator, List, TypedDict, Union

import aiohttp

from .base import OutputBase


def parse_line(line: str) -> list:
    return next(csv.reader([line]))


class Metric(TypedDict):
    metric: Dict[str, str]
    values: List[Union[int, float, None]]
    timestamps: List[int]


class Victoria(OutputBase):
    MAX_ATTEMPTS = 3
    def __init__(self, url: str):
        self.url = url

    async def import_data(self, metrics: Generator[bytes, None, None]):
        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            async with aiohttp.ClientSession() as session:
                try:
                    response = await session.post(f'{self.url}/api/v1/import', data=b'\n'.join(metrics))
                except aiohttp.ClientError:
                    print(f'{attempt}/{self.MAX_ATTEMPTS} Cannot connect to victoriametrics')
                    if attempt < self.MAX_ATTEMPTS:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        raise
                else:
                    if not response.ok:
                        print(f'{attempt}/{self.MAX_ATTEMPTS} Cannot send data to victoriametrics, status {response.status}')
                        if attempt < self.MAX_ATTEMPTS:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            response.raise_for_status()
                    else:
                        break
