import csv
from typing import Dict, List, Sequence, TypedDict, Union

import aiohttp

from .base import OutputBase


def parse_line(line: str) -> list:
    return next(csv.reader([line]))


class Metric(TypedDict):
    metric: Dict[str, str]
    values: List[Union[int, float, None]]
    timestamps: List[int]


class Victoria(OutputBase):
    def __init__(self, url: str):
        self.url = url

    async def import_data(self, metrics: Sequence[bytes]):
        async with aiohttp.ClientSession() as session:
            response = await session.post(f'{self.url}/api/v1/import', data=b'\n'.join(metrics))
            print(response.status)
            response.raise_for_status()
            if response.content_length:
                print(response.content)
