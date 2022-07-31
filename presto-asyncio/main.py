import asyncio

from prestodb.exceptions import HttpError

from presto.presto_query import logger, PrestoQuery
from presto.presto_request import PrestoRequest


async def execute_presto(query_context):
    request = PrestoRequest(
        host=query_context['host'],
        port=query_context['port'],
        user=query_context['user'],
        catalog=query_context['catalog'],
        schema=query_context['schema'],
        source=query_context['source'],
        next_uri=query_context['next_uri']
    )
    with request as req:
        query = PrestoQuery(req, sql=query_context['query'])

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

            return query_context
        except HttpError as err:
            logger.exception("HttpError")
        except BaseException:
            logger.exception("Unknown Exception")
        finally:
            return query_context


if __name__ == "__main__":
    query_context = dict(
        port=443,
        catalog='dq-obs',
        schema='default',
        source='python test',
        next_uri='',
        query='select * from deletetest limit 100'
    )
    asyncio.run(execute_presto(query_context))
