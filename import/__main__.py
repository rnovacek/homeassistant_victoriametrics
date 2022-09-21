import sys
import asyncio
from collections import defaultdict
import json
from typing import Dict, List, Tuple, Union
import zoneinfo
from datetime import datetime

try:
    import tomllib  # type: ignore
except ModuleNotFoundError:
    import tomli as tomllib

import dateutil.parser

from .base import InputBase, OutputBase
from .influx import Influx
from .victoria import Metric, Victoria

utc = zoneinfo.ZoneInfo('UTC')
local = zoneinfo.ZoneInfo('Europe/Prague')


def transform_tag(tag: str):
    if tag == '_measurement':
        return 'unit_of_measurement'
    return tag.removesuffix('_str')


class Importer:
    def __init__(self, config: dict, input: InputBase, output: OutputBase):
        self.config = config
        self.input = input
        self.output = output

        self.blacklist_entities = set(self.config.get('blacklist_entities', None) or [])
        self.blacklist_tags = set(self.config.get('blacklist_tags', None) or [])

    async def generate(self, start: datetime, end: datetime, prefix: str):
        async for entity in self.input.get_unique_entities(start, end):
            if entity in self.blacklist_entities:
                print(f'Entity {entity} skipped')
                continue

            print('Processing INFLUX entity', entity)

            datatypes = None
            headers = None

            metrics: Dict[str, Metric] = defaultdict(lambda: {
                'metric': {
                    '__name__': '',
                },
                'values': [],
                'timestamps': [],
            })

            async for entry in self.input.export_entity(entity, start, end):
                if len(entry) <= 1:
                    continue
                elif entry[0] == '#datatype':
                    datatypes = entry
                elif entry[1] == 'result':
                    headers = entry
                else: # data
                    assert datatypes
                    assert headers

                    key_values: List[Tuple[str, Union[float, None]]] = []
                    tags: Dict[str, str] = {}
                    domain = None
                    entity_id = None
                    timestamp = None

                    for i, value in enumerate(entry):
                        datatype = datatypes[i]
                        header = headers[i]
                        if header in ['', 'result', 'table']:
                            continue
                        elif header == '_time':
                            dt = dateutil.parser.isoparse(value)
                            timestamp = int(dt.strftime('%s')) * 1000
                        elif header == 'domain':
                            domain = value
                        elif header == 'entity_id':
                            entity_id = value
                        elif datatype == 'double' and value != '':
                            float_value = float(value)

                            if header == 'value':
                                key_values.append(('value', float_value))
                            else:
                                key_values.append((header, float_value))
                        else:
                            key = transform_tag(header)
                            if key in self.blacklist_tags:
                                continue
                            tags[key] = value

                    if not key_values:
                        # If there is no numeric state, use 0 so we at least post attributes
                        key_values.append(('value', 0))

                    assert domain
                    assert entity_id
                    assert timestamp

                    for key, value in key_values:
                        metric_name = f'{prefix}.{domain}.{entity_id}.{key.replace(" ", "_")}'
                        tag = ';'.join(f'{k}={v}' for k, v in tags.items())
                        metric_with_tags = f'{metric_name};{tag}'
                        metric = metrics[metric_with_tags]
                        metric['metric']['__name__'] = metric_name
                        metric['values'].append(value)
                        metric['timestamps'].append(timestamp)

                        for tag_key, tag_value in tags.items():
                            metric['metric'][tag_key] = tag_value

            yield [json.dumps(metric, indent=None).encode('utf-8') for metric in metrics.values()]

            print('DONE')

    async def process(self, start: datetime, end: datetime, prefix: str):
        async for jsonl in self.generate(start, end, prefix):
            await self.output.import_data(jsonl)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} config.toml')
        sys.exit(1)

    with open(sys.argv[1], 'rb') as f:
        config = tomllib.load(f)

    if 'input' not in config:
        print('[input] configuration not found', file=sys.stderr)
        sys.exit(1)

    input_type = config['input'].pop('type')
    if input_type == 'influxV2':
        input = Influx(**config['input'])
    else:
        print('Invalid input type, only "influxV2" is supported', file=sys.stderr)
        sys.exit(1)

    if 'output' not in config:
        print('[output] configuration not found', file=sys.stderr)
        sys.exit(1)

    output_type = config['output'].pop('type')
    if output_type == 'victoriametrics':
        output = Victoria(**config['output'])
    else:
        print('Invalid output type, only "victoriametrics" is supported', file=sys.stderr)
        sys.exit(1)

    start = config['start']
    end = config['end']
    if end == 'now':
        end = datetime.now(tz=zoneinfo.ZoneInfo('UTC'))

    importer = Importer(config, input, output)

    asyncio.run(importer.process(start, end, prefix=config.get('prefix') or 'ha'))

