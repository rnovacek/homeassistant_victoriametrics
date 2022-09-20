
import csv

import aiohttp

from .base import InputBase


class Influx(InputBase):
    def __init__(self, url: str, org_id: str, token: str, bucket: str):
        self.url = url
        self.org_id = org_id
        self.token = token
        self.bucket = bucket

    async def get_unique_entities(self, start, end):
        query = f'''
from(bucket: "homeassistant")
    |> range(start: {start.isoformat()}, stop: {end.isoformat()})
    |> keep(columns: ["entity_id"])
    |> unique(column: "entity_id")
'''
        print(query)
        async for row in self.get_data({
            'query': query,
        }):
            if len(row) == 4 and row[3] != 'entity_id':
                yield row[3]

    async def export_entity(self, entity, start, end):
        query = f'''
from(bucket: "{self.bucket}")
    |> range(start: {start.isoformat()}, stop: {end.isoformat()})
    |> filter(fn: (r) => r["entity_id"] == "{entity}")
    |> pivot(
        rowKey: ["_time"],
        columnKey: ["_field"],
        valueColumn: "_value"
    )
    |> drop(columns: ["_start", "_stop"])
'''
        print(query)
        async for row in self.get_data({
            'query': query,
            'dialect': {
                'annotations': ['datatype'],
            },
        }):
            yield row

    async def get_data(self, query: dict):
        async with aiohttp.ClientSession() as session:
            async with session.post(f'{self.url}/api/v2/query?orgID={self.org_id}', headers={
                'Authorization': f'Token {self.token}',
                'Content-Type': 'application/json',
                'Accept-Encoding': 'gzip',
            }, json=query) as response:
                response.raise_for_status()
                async for line in response.content:
                    parsed_line = next(csv.reader([line.decode('utf-8')]))
                    yield parsed_line
