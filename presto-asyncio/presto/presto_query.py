
import logging

from prestodb import exceptions

from typing import Any, Dict, List, Optional, Text, Tuple, Union  # NOQA for mypy types

from presto.presto_request import PrestoRequest
from presto.presto_result import PrestoResult

logger = logging.getLogger(__name__)


class PrestoQuery(object):
    """Represent the execution of a SQL statement by Presto."""

    def __init__(
            self,
            request: PrestoRequest,  # type: PrestoRequest
            sql=None  # type: Text
    ):
        # type: (...) -> None

        self.auth_req = request.auth_req  # type: Optional[Request]
        self.credentials = request.credentials  # type: Optional[Credentials]
        self.query_id = None  # type: Optional[Text]

        self._stats = {}  # type: Dict[Any, Any]
        self._warnings = []  # type: List[Dict[Any, Any]]
        self._columns = None  # type: Optional[List[Text]]
        self._finished = False
        self._cancelled = False
        self._request = request
        self._sql = sql
        self._result = []
        # self._result = PrestoResult(self)

    @property
    def columns(self):
        return self._columns

    @property
    def stats(self):
        return self._stats

    @property
    def warnings(self):
        return self._warnings

    @property
    def result(self):
        return self._result

    async def execute(self):
        # type: () -> PrestoResult

        if self._cancelled:
            raise exceptions.PrestoUserError("Query has been cancelled", self.query_id)

        if self.credentials is not None and not self.credentials.valid:
            self._request.http_session.headers.update(self._request.get_oauth_token())

        if self._request.next_uri:
            response = await self._request.get(self._request.next_uri)
        else:
            response = await self._request.post(self._sql)
        status = await self._request.process(response)
        self.query_id = status.id
        if status.columns:
            self._columns = status.columns
        self._stats.update({u"queryId": self.query_id})
        self._stats.update(status.stats)
        self._warnings = getattr(status, "warnings", [])
        if status.next_uri is None:
            self._finished = True
        # self._result = PrestoResult(self, status.rows)
        self._result = status.rows
        return self._result

    async def fetch(self):
        # type: () -> List[List[Any]]
        """Continue fetching data for the current query_id"""
        response = await self._request.get(self._request.next_uri)
        status = await self._request.process(response)
        if status.columns:
            self._columns = status.columns
        self._stats.update(status.stats)
        logger.debug(status)
        if status.next_uri is None:
            self._finished = True
        return status.rows

    async def cancel(self):
        # type: () -> None
        """Cancel the current presto"""
        if self.query_id is None or self.is_finished():
            return

        self._cancelled = True
        url = self._request.get_url("/v1/presto/{}".format(self.query_id))
        logger.debug("cancelling presto: %s", self.query_id)
        response = await self._request.delete(url)
        logger.info(response)
        if response.status == 204:
            logger.debug("presto cancelled: %s", self.query_id)
            return
        self._request.raise_response_error(response)

    def is_finished(self):
        # type: () -> bool
        return self._finished
