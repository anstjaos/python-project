import json
import os

from aiofile import async_open


async def save_columns_to_csv_file(columns, query_id):
    if os.path.isfile(query_id + '.csv'):
        return

    data = {
        'columns': []
    }
    for column in columns:
        data['columns'].append({
            'name': column['name'],
            'type': column['type']
        })
    async with async_open(query_id + '.csv', 'w+') as afp:
        await afp.write(json.dumps(data))


async def save_rows_to_csv_file(rows, query_id):
    async with async_open(query_id + '.csv', 'r') as afp:
        data = json.loads(await afp.read())

    if 'rows' not in data:
        data['rows'] = rows
    else:
        data['rows'].append(rows)

    async with async_open(query_id + '.csv', 'w+') as afp:
        await afp.write(json.dumps(data))


async def save_columns_to_json_file(columns, query_id):
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


async def save_rows_to_json_file(rows, query_id):
    async with async_open(query_id + '.json', 'r') as afp:
        data = json.loads(await afp.read())

    if 'rows' not in data:
        data['rows'] = rows
    else:
        data['rows'].append(rows)

    async with async_open(query_id + '.json', 'w+') as afp:
        await afp.write(json.dumps(data))
