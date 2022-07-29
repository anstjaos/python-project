import functools
import logging
from typing import Any, Dict, List, Optional, Text, Tuple, Union  # NOQA for mypy types

import aiohttp
import prestodb
import six.moves.urllib_parse as parse
from aiohttp import ClientPayloadError
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import BaseTimerContext
from aiohttp.http_parser import HttpResponseParser
from prestodb import exceptions, constants
from prestodb.client import MAX_ATTEMPTS, ClientSession, PROXIES, get_header_values, get_session_property_values, \
    PrestoStatus
from prestodb.transaction import NO_TRANSACTION

logger = logging.getLogger(__name__)


def is_redirect(http_response):
    # type: (aiohttp.ClientResponse) -> bool
    return 'location' in http_response.headers and http_response.status in (301, 302, 303, 307, 308)


# Set allowed header size to 1MB - https://github.com/trinodb/trino/issues/8797
# https://github.com/aio-libs/aiohttp/issues/2988
class PrestoResponseHandler(ResponseHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_response_params(
        self,
        *,
        timer: Optional[BaseTimerContext] = None,
        skip_payload: bool = False,
        read_until_eof: bool = False,
        auto_decompress: bool = True,
        read_timeout: Optional[float] = None,
        read_bufsize: int = 2 ** 16,
    ) -> None:
        self._skip_payload = skip_payload

        self._read_timeout = read_timeout
        self._reschedule_timeout()

        self._parser = HttpResponseParser(
            self,
            self._loop,
            read_bufsize,
            timer=timer,
            payload_exception=ClientPayloadError,
            response_with_body=not skip_payload,
            read_until_eof=read_until_eof,
            auto_decompress=auto_decompress,
            max_line_size=8190 * 4,
            max_headers=32768 * 4,
            max_field_size=1_048_576,  # 1MB
        )

        if self._tail:
            data, self._tail = self._tail, b""
            self.data_received(data)


class PrestoTCPConnector(aiohttp.TCPConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._factory = functools.partial(PrestoResponseHandler, loop=self._loop)
# End header size fix


class PrestoRequest(object):
    """
    Manage the HTTP requests of a Presto presto.

    :param host: name of the coordinator
    :param port: TCP port to connect to the coordinator
    :param user: associated with the presto. It is useful for access control
                 and presto scheduling.
    :param source: associated with the presto. It is useful for access
                   control and presto scheduling.
    :param catalog: to presto. The *catalog* is associated with a Presto
                    connector. This variable sets the default catalog used
                    by SQL statements. For example, if *catalog* is set
                    to ``some_catalog``, the SQL statement
                    ``SELECT * FROM some_schema.some_table`` will actually
                    presto the table
                    ``some_catalog.some_schema.some_table``.
    :param schema: to presto. The *schema* is a logical abstraction to group
                   table. This variable sets the default schema used by
                   SQL statements. For eample, if *schema* is set to
                   ``some_schema``, the SQL statement
                   ``SELECT * FROM some_table`` will actually presto the
                   table ``some_catalog.some_schema.some_table``.
    :param session_properties: set specific Presto behavior for the current
                               session. Please refer to the output of
                               ``SHOW SESSION`` to check the available
                               properties.
    :param http_headers: HTTP headers to post/get in the HTTP requests
    :param http_scheme: "http" or "https"
    :param auth: class that manages user authentication. ``None`` means no
                 authentication.
    :max_attempts: maximum number of attempts when sending HTTP requests. An
                   attempt is an HTTP request. 5 attempts means 4 retries.
    :request_timeout: How long (in seconds) to wait for the server to send
                      data before giving up, as a float or a
                      ``(connect timeout, read timeout)`` tuple.

    The client initiates a presto by sending an HTTP POST to the
    coordinator. It then gets a response back from the coordinator with:
    - An URI to presto to get the status for the presto and the remaining
      data
    - An URI to get more information about the execution of the presto
    - Statistics about the current presto execution

    Please refer to :class:`PrestoStatus` to access the status returned by
    :meth:`PrestoRequest.process`.

    When the client makes an HTTP request, it may encounter the following
    errors:
    - Connection or read timeout:
      - There is a network partition and TCP segments are
        either dropped or delayed.
      - The coordinator stalled because of an OS level stall (page allocation
        stall, long time to page in pages, etc...), a JVM stall (full GC), or
        an application level stall (thread starving, lock contention)
    - Connection refused: Configuration or runtime issue on the coordinator
    - Connection closed:

    As most of these errors are transient, the question the caller should set
    retries with respect to when they want to notify the application that uses
    the client.
    """

    http = aiohttp

    HTTP_EXCEPTIONS = (
        http.ServerConnectionError,  # type: ignore
        http.ServerTimeoutError,  # type: ignore
    )

    def __init__(
            self,
            host,  # type: Text
            port,  # type: int
            user,  # type: Text
            next_uri=None,  # type: Text
            source=None,  # type: Text
            catalog=None,  # type: Text
            schema=None,  # type: Text
            session_properties=None,  # type: Optional[Dict[Text, Any]]
            http_session=None,  # type: Any
            http_headers=None,  # type: Optional[Dict[Text, Text]]
            transaction_id=NO_TRANSACTION,  # type: Optional[Text]
            http_scheme=constants.HTTP,  # type: Text
            auth=constants.DEFAULT_AUTH,  # type: Optional[Any]
            redirect_handler=prestodb.redirect.GatewayRedirectHandler(),
            max_attempts=MAX_ATTEMPTS,  # type: int
            request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,  # type: Union[float, Tuple[float, float]]
            handle_retry=exceptions.RetryWithExponentialBackoff(),
            service_account_file=None,
            verify=False  # type: bool
    ):
        # type: (...) -> None
        self._client_session = ClientSession(
            catalog,
            schema,
            source,
            user,
            session_properties,
            http_headers,
            transaction_id,
        )

        self._host = host
        self._port = port
        self._next_uri = next_uri  # type: Optional[Text]

        if http_session is not None:
            self._http_session = http_session
            self._close_session = False
        else:
            # mypy cannot follow module import
            self._http_session = self.http.ClientSession(connector=PrestoTCPConnector(verify_ssl=verify))  # type: ignore
            self._close_session = True
        self.credentials = None
        self.auth_req = None
        if service_account_file is not None:
            import google.auth.transport.requests
            from google.oauth2 import service_account

            self.auth_req = google.auth.transport.requests.Request()
            self.credentials = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=[constants.GCS_READ_ONLY]
            )
            self._http_session.headers.update(self.get_oauth_token())

        self._http_session.headers.update(self.http_headers)
        self._exceptions = self.HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            if http_scheme == constants.HTTP:
                raise ValueError("cannot use authentication with HTTP")
            self._auth.set_http_session(self._http_session)
            self._exceptions += self._auth.get_exceptions()

        self._redirect_handler = redirect_handler
        self._request_timeout = request_timeout
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts
        self._http_scheme = http_scheme
        self._verify = verify

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _close_http_session(self):
        if self._http_session and self._close_session:
            if not self._http_session.closed:
                await self._http_session.connector.close()
            self._http_session = None

    async def close(self):
        await self._close_http_session()

    @property
    def transaction_id(self):
        return self._client_session.transaction_id

    @transaction_id.setter
    def transaction_id(self, value):
        self._client_session.transaction_id = value

    @property
    def http_headers(self):
        # type: () -> Dict[Text, Text]
        header_session = ",".join(
            # ``name`` must not contain ``=``
            "{}={}".format(name, parse.quote(str(value)))
            for name, value in self._client_session.properties.items()
        )

        headers = {
            constants.HEADER_CATALOG: self._client_session.catalog,
            constants.HEADER_SCHEMA: self._client_session.schema,
            constants.HEADER_SOURCE: self._client_session.source,
            constants.HEADER_USER: self._client_session.user,
            constants.HEADER_SESSION: header_session
        }

        # merge custom http headers
        for key in self._client_session.headers:
            if key in headers.keys():
                raise ValueError("cannot override reserved HTTP header {}".format(key))
        headers.update(self._client_session.headers)

        transaction_id = self._client_session.transaction_id
        headers[constants.HEADER_TRANSACTION] = transaction_id

        return {k: v for k, v in headers.items() if v is not None}

    @property
    def max_attempts(self):
        # type: () -> int
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value):
        # type: (int) -> None
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            self._delete = self._http_session.delete
            return

        with_retry = exceptions.retry_with(
            self._handle_retry,
            exceptions=self._exceptions,
            conditions=(
                # need retry when there is no exception but the status code is 503
                lambda response: getattr(response, "status", None) == 503,
            ),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)
        self._delete = with_retry(self._http_session.delete)

    def get_url(self, path):
        # type: (Text) -> Text
        return f"{self._http_scheme}://{self._host}:{self._port}{path}"

    @property
    def statement_url(self):
        # type: () -> Text
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self):
        # type: () -> Text
        return self._next_uri

    async def post(self, sql):
        data = sql.encode("utf-8")
        http_headers = self.http_headers

        http_response = await self._post(
            self.statement_url,
            data=data,
            headers=http_headers,
            timeout=self._request_timeout,
            allow_redirects=self._redirect_handler is None,
            proxy=PROXIES,
        )

        if self._redirect_handler is not None:
            while http_response is not None and is_redirect(http_response):
                location = http_response.headers["Location"]
                url = self._redirect_handler.handle(location)
                logger.info(f"redirect {http_response.status} from {location} to {url}")
                http_response = self._post(
                    url,
                    data=data,
                    headers=http_headers,
                    timeout=self._request_timeout,
                    allow_redirects=False,
                    proxy=PROXIES,
                )
        return http_response

    async def get(self, url):
        return await self._get(
            url,
            headers=self.http_headers,
            timeout=self._request_timeout,
            proxy=PROXIES,
        )

    async def delete(self, url):
        return await self._delete(url, timeout=self._request_timeout, proxy=PROXIES)

    def _process_error(self, error, query_id):
        error_type = error["errorType"]
        if error_type == "EXTERNAL":
            raise exceptions.PrestoExternalError(error, query_id)
        elif error_type == "USER_ERROR":
            return exceptions.PrestoUserError(error, query_id)

        return exceptions.PrestoQueryError(error, query_id)

    def raise_response_error(self, http_response):
        # type: (aiohttp.ClientResponse) -> None
        if http_response.status == 503:
            raise exceptions.Http503Error("error 503: service unavailable")

        raise exceptions.HttpError(
            "error {}{}".format(
                http_response.status,
                ": {}".format(http_response.content) if http_response.content else "",
            )
        )

    async def process(self, http_response):
        # type: (aiohttp.ClientResponse) -> PrestoStatus
        if not http_response.ok:
            self.raise_response_error(http_response)

        http_response.encoding = "utf-8"
        response = await http_response.json()
        logger.debug(f"HTTP {http_response.status}: {response}")
        if "error" in response:
            raise self._process_error(response["error"], response.get("id"))

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(
                    http_response.headers, constants.HEADER_CLEAR_SESSION
            ):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(
                    http_response.headers, constants.HEADER_SET_SESSION
            ):
                self._client_session.properties[key] = value

        self._next_uri = response.get("nextUri")

        return PrestoStatus(
            id=response["id"],
            stats=response["stats"],
            warnings=response.get("warnings", []),
            info_uri=response["infoUri"],
            next_uri=self._next_uri,
            rows=response.get("data", []),
            columns=response.get("columns"),
        )

    @property
    def http_session(self):
        return self._http_session

    def get_oauth_token(self):
        self.credentials.refresh(self.auth_req)
        return {
            constants.PRESTO_EXTRA_CREDENTIAL: f"{constants.GCS_CREDENTIALS_OAUTH_TOKEN_KEY} = {self.credentials.token}"
        }
