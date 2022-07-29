class PrestoResult(object):
    """
    Represent the result of a Trino query as an iterator on rows.
    This class implements the iterator protocol as a generator type
    https://docs.python.org/3/library/stdtypes.html#generator-types
    """

    def __init__(self, query, rows=None):
        self._query = query
        self._rows = rows or []
        self._rownumber = 0

    @property
    def rownumber(self):
        # type: () -> int
        return self._rownumber

    def __aiter__(self):
        # Easier then manually implementing __anext__
        async def gen():
            # Initial fetch from the first POST request
            for row in self._rows:
                self._rownumber += 1
                yield row
            self._rows = None
            # Subsequent fetches from GET requests until next_uri is empty.
            while not self._query.is_finished():
                rows = await self._query.fetch()
                for row in rows:
                    self._rownumber += 1
                    yield row

        return gen()

    @property
    def response_headers(self):
        return self._query.response_headers
