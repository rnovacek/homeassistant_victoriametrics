
from datetime import datetime
from typing import AsyncGenerator, Generator, List, Sequence


class InputBase:
    async def get_unique_entities(self, start: datetime, end: datetime) -> AsyncGenerator[str, None]:
        raise NotImplementedError()
        yield ''

    async def export_entity(self, entity: str, start: datetime, end: datetime) -> AsyncGenerator[List[str], None]:
        raise NotImplementedError()
        yield []


class OutputBase:
    async def import_data(self, metrics: Generator[bytes, None, None]):
        raise NotImplementedError()
