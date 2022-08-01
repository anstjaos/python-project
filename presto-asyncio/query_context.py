class QueryContext(object):

    def __init__(
            self,
            host,
            port,
            user,
            catalog,
            schema,
            source,
            query,
            next_uri=None
    ):
        self._host = host
        self._port = port
        self._user = user
        self._catalog = catalog
        self._schema = schema
        self._source = source
        self._next_uri = next_uri
        self._query = query

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def catalog(self):
        return self._catalog

    @property
    def schema(self):
        return self._schema

    @property
    def source(self):
        return self._source

    @property
    def next_uri(self):
        return self._next_uri

    @property
    def query(self):
        return self._query
