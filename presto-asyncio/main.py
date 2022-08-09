import asyncio
import json
import logging
import os.path
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Tuple, Any, Dict

import pytz
from aiofile import async_open
from prestodb.exceptions import HttpError

from presto.presto_query import PrestoQuery
from presto.presto_request import PrestoRequest
from query_context import QueryContext

logger = logging.getLogger(__name__)

INF = float("inf")
NEGATIVE_INF = float("-inf")
NAN = float("nan")


async def execute_presto(query_context: QueryContext):
    request = PrestoRequest(
        host=query_context.host,
        port=query_context.port,
        user=query_context.user,
        catalog=query_context.catalog,
        schema=query_context.schema,
        source=query_context.source,
        next_uri=query_context.next_uri,
    )
    async with request as req:
        query = PrestoQuery(req, sql=query_context.query)

        try:
            rows = []
            columns = []

            await query.execute()
            if query.result:
                rows.extend(query.result)
            if query.columns:
                columns = query.columns

            while not query.is_finished():
                results = await query.fetch()
                if not columns and query.columns:
                    columns = query.columns
                    await save_columns(columns, query.query_id)

                if results:
                    convert_results = []
                    for row in results:
                        convert_results.append(list(map(map_to_python_type, zip(row, columns))))
                    rows.extend(convert_results)
                    await save_rows(convert_results, query.query_id)

            logger.info(f"row size:{len(rows)}")
            logger.info(f"columns:{columns}")
            logger.info(f"query_id:{query.query_id}")
            return query_context
        except HttpError as err:
            logger.exception("HttpError")
        except BaseException:
            logger.exception("Unknown Exception")
        finally:
            return query_context


async def save_columns(columns, query_id):
    if os.path.isfile(query_id + '.json'):
        return

    data = {
        'columns': []
    }
    for column in columns:
        data['columns'].append({
            'name': column['name'],
            'type': column['type']
        })
    async with async_open(query_id + '.json', 'w+') as afp:
        await afp.write(json.dumps(data))


async def save_rows(rows, query_id):
    async with async_open(query_id + '.json', 'r') as afp:
        data = json.loads(await afp.read())

    if 'rows' not in data:
        data['rows'] = rows
    else:
        data['rows'].append(rows)

    async with async_open(query_id + '.json', 'w+') as afp:
        await afp.write(json.dumps(data))


def map_to_python_type(item: Tuple[Any, Dict]) -> Any:
    (value, data_type) = item

    if value is None:
        return None

    raw_type = data_type["typeSignature"]["rawType"]

    try:
        if isinstance(value, list):
            if raw_type == "array":
                raw_type = {
                    "typeSignature": data_type["typeSignature"]["arguments"][0]["value"]
                }
                return [map_to_python_type((array_item, raw_type)) for array_item in value]
            if raw_type == "row":
                raw_types = map(lambda arg: arg["value"], data_type["typeSignature"]["arguments"])
                return tuple(
                    map_to_python_type((array_item, raw_type))
                    for (array_item, raw_type) in zip(value, raw_types)
                )
            return value
        if isinstance(value, dict):
            raw_key_type = {
                "typeSignature": data_type["typeSignature"]["arguments"][0]["value"]
            }
            raw_value_type = {
                "typeSignature": data_type["typeSignature"]["arguments"][1]["value"]
            }
            return {
                map_to_python_type((key, raw_key_type)):
                    map_to_python_type((value[key], raw_value_type))
                for key in value
            }
        elif "decimal" in raw_type:
            return Decimal(value)
        elif raw_type == "double":
            if value == 'Infinity':
                return INF
            elif value == '-Infinity':
                return NEGATIVE_INF
            elif value == 'NaN':
                return NAN
            return value
        elif raw_type == "date":
            return datetime.strptime(value, "%Y-%m-%d").date()
        elif raw_type == "timestamp with time zone":
            dt, tz = value.rsplit(' ', 1)
            if tz.startswith('+') or tz.startswith('-'):
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f %z")
            return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=pytz.timezone(tz))
        elif "timestamp" in raw_type:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        elif "time with time zone" in raw_type:
            matches = re.match(r'^(.*)([\+\-])(\d{2}):(\d{2})$', value)
            assert matches is not None
            assert len(matches.groups()) == 4
            if matches.group(2) == '-':
                tz = -timedelta(hours=int(matches.group(3)), minutes=int(matches.group(4)))
            else:
                tz = timedelta(hours=int(matches.group(3)), minutes=int(matches.group(4)))
            return datetime.strptime(matches.group(1), "%H:%M:%S.%f").time().replace(tzinfo=timezone(tz))
        elif "time" in raw_type:
            return datetime.strptime(value, "%H:%M:%S.%f").time()
        else:
            return value
    except ValueError as e:
        error_str = f"Could not convert '{value}' into the associated python type for '{raw_type}'"
        raise TypeError(error_str) from e


if __name__ == "__main__":
    query_context = QueryContext(
        host='10.161.166.58',
        port=8080,
        user='user',
        catalog='memory',
        schema='default',
        source='python test',
        query='select * from test15'
    )
    # query_context = QueryContext(
    #     host='83c0b5a6-f4d9-40a6-9923-bbec44014e15.cluster-dataquery.alpha-nhncloudservice.com',
    #     port=443,
    #     user='moonseo.kim@nhn.com',
    #     catalog='df-mysql',
    #     schema='hello',
    #     source='python test',
    #     query='select * from hello limit 1000'
    # )
    logging.basicConfig(level=logging.INFO)
    asyncio.run(execute_presto(query_context))
