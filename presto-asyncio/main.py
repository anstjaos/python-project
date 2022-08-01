import asyncio

import prestodb
from prestodb.exceptions import HttpError

from presto.presto_query import PrestoQuery
from presto.presto_request import PrestoRequest
from query_context import QueryContext
import logging
from aiofile import async_open

logger = logging.getLogger(__name__)


async def execute_presto(query_context: QueryContext):
    request = PrestoRequest(
        host=query_context.host,
        port=query_context.port,
        user=query_context.user,
        catalog=query_context.catalog,
        schema=query_context.schema,
        source=query_context.source,
        next_uri=query_context.next_uri
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

                if results:
                    rows.extend(results)

            logger.info(f"rows:{rows}")
            logger.info(f"columns:{columns}")
            logger.info(f"query_id:{query.query_id}")
            await save_file(rows, columns, query.query_id)
            return query_context
        except HttpError as err:
            logger.exception("HttpError")
        except BaseException:
            logger.exception("Unknown Exception")
        finally:
            return query_context


async def save_file(rows, columns, query_id):
    async with async_open(query_id + '.txt', 'w+') as afp:
        await afp.write("columns\n")
        for column in columns:
            await afp.write("%s\n" % column)
        await afp.write("rows\n")
        for row in rows:
            await afp.write('row num: %s\n' % row[0])
            for data in row[1]:
                await afp.write("%s " % data)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    asyncio.run(execute_presto(query_context))
