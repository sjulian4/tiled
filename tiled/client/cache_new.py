import enum
import json
import os
import sqlite3
import sys
import typing as tp
from contextlib import closing
from datetime import datetime
from hashlib import sha256
from pathlib import Path
import uuid

import httpcore
import platformdirs
from hishel import (  # instead of BaseStorage there is not SyncBaseStorage and AsyncBaseStorage. There is no BaseSerializer
    SyncBaseStorage, RequestMetadata, ResponseMetadata
)
from hishel import (
    Entry, EntryMeta
)
from httpcore import Request, Response

from .logger import logger
from .utils import SerializableLock

CACHE_DATABASE_SCHEMA_VERSION = 2

HEADERS_ENCODING = "iso-8859-1"
KNOWN_RESPONSE_EXTENSIONS = ("http_version", "reason_phrase")
KNOWN_REQUEST_EXTENSIONS = ("timeout", "sni_hostname")

# This is currently only used for checking SQlite thread-safety
PY311 = sys.version_info >= (3, 11)


def create_cache_key(request: Request, body: bytes = b"") -> str:
    """
    Generate a Cache key. A Cache key contains the method, url, and request body.
    :param request: An HTTP request
    :type request: httpcore.Request
    :param body: The body of the request. To be included for e.g. POST
    :type body: tp.Optional[bytes]
    """
    method = request.method.decode()  # so this takes in the request and decodes it
    url = request.url.decode()  # check that this is the full URL
    body_hasher = sha256()
    body_hasher.update(body)
    body_hashed = body_hasher.hexdigest()
    return f"{method}|{url}|{body_hashed}"  # so the cache key is based on the request
    # so i'm thinking the idea after this is that this cache key is used in Hishel 'cause it overrides the hishel key generation


def measure_entry_size(request, response, response_content=None, request_content=None):
    # httpcore exception that is == httpx.ResponseNotRead()
    # Trace out the way this works for a streaming response
    # Also handle streaming request
    if hasattr(response, "_content"):
        size = len(response.content)
    elif response_content is None:
        raise Exception
    else:
        size = len(response_content)

    if hasattr(request, "_content"):
        size += len(request.content)
    elif request_content is None:
        raise Exception
    else:
        size += len(request_content)
    return size
    # TODO I think we need to add the handling of the streaming here. This doesn't have a streaming aspect in the original I dont think, however maybe it's because there isn't a dump/load in this version?
    # Why does streaming matter here? maybe it is because the size can change concurrently? like if data entries keep being added then the size changes?
    # OG version: the size is measured in cached items, so every cached item increases the size by 1


def with_safe_threading(fn):
    """
    Ensure thread-safe SQLite access

    If we can check that the underlying SQLite module
    is built with thread-safety, then no need to lock.

    If we cannot check or the check is false, use a lock
    to ensure the database isn't accessed concurrently.
    """

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        sqlite_is_safe = sqlite3.threadsafety == ThreadingMode.SERIALIZED
        lock_is_mine = False

        if not (
            PY311 and sqlite_is_safe
        ):  # if the python version is less than 3.11 or if it is not sqlite safe,
            lock_is_mine = obj._lock.acquire()  # then acquire a lock
        try:
            result = fn(obj, *args, **kwargs)  # carry out the function as intended
        finally:
            if (
                lock_is_mine and obj._lock.locked()
            ):  # if there is a lock and it is locked
                obj._lock.release()  # release the object from being locked (im assuming this is unlocking it after the function runs)
        return result  # return whatever the function returned
        # I think this is essentially locking some value until the function finishes running. wait this reminds me of 320 when we would prevent multiple processes from occurring at the same time and messing with values.

    return wrapper
    # unlike the old version, this version uses sqlite, which is an embedded database.
    # so it's saying that if the sqlite3 threadsafety is equal to 3, then the sqlite is safe


class ThreadingMode(enum.IntEnum):
    """
    Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety
    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3


class TiledSerializer:
    def dumps(
        self,
        response: Response,
        request: Request,
        entry_meta: EntryMeta,
        response_content,
        request_content,
    ) -> tp.Union[str, bytes]:

        status_code = response.status
        headers = json.dumps(
            [
                (key.decode(HEADERS_ENCODING), value.decode(HEADERS_ENCODING))
                for key, value in response.headers
            ]
        )  # since request headers is in request_serialized, just doing response here
        body = json.dumps(
            {
                "Response": response_content,
                "Request": request_content, 
            }# this is here to allow for streaming to happen 
        )
        is_stream = (
            request.stream or response.stream
        )  # I don't think this is a boolean but should be
        encoding = "ascii" #TODO: Is this right to just be ascii?
        size = measure_entry_size(request, response, response_content, request_content)
        method = request.method.decode()
        url = (
            request.url.decode()
        )  # this may need to change based on the old JSON serializer https://github.com/karpetrosyan/hishel/blob/39b6e580a078731ed4fdccac33739ea067e9daa4/hishel/_serializers.py
        request_serialized = json.dumps(
            {
                "method": method,
                "url": url,
                "headers": [
                    (key.decode(HEADERS_ENCODING), value.decode(HEADERS_ENCODING))
                    for key, value in request.headers
                ],
                "extensions": {
                    key: value
                    for key, value in request.extensions.items()
                    if key in KNOWN_REQUEST_EXTENSIONS
                },
            }
        )
        created_at = entry_meta.created_at
        time_last_accessed = datetime.now().timestamp()  # this form in _store
        return (
            status_code,
            headers,
            body,
            is_stream,
            encoding,
            size,
            request_serialized,
            created_at,
            time_last_accessed,
        )

    def loads(
        self, data: tp.Union[str, bytes]
    ) -> Entry:

        #TODO: what do we do with is_stream now that it's Entry?
       
        loaded_response = Response(
            status=data[2],
            headers=[
                (key.encode(HEADERS_ENCODING), value.encode(HEADERS_ENCODING))
                for key, value in json.loads(data[3])
            ],
            content=json.loads(data[4])["Response"].encode("ascii"), #converts it to a dicts, then takes the Response, then encodes it to bytes
            stream=data[5],
        )

        # loads it and then turns it into a Request object
        request_unpacked = json.loads(data[7])
        loaded_request = Request(
            method=request_unpacked["method"],
            url=request_unpacked["url"],
            headers=[
                (key.encode(HEADERS_ENCODING), value.encode(HEADERS_ENCODING))
                for key, value in request_unpacked["headers"]
            ],
            content = json.loads(data[3])["Request"].encode("ascii")
            # TODO Do I need an extensions? Or just let it default to the None?
        )

        loaded_entry_meta = EntryMeta(created_at = datetime.fromtimestamp(data[8]), deleted_at = None)  # this is turning it back to datetime from the timestamp it was stored as in _store

        # uuid.UUID(data[1]) converts from string to UUID
        loaded_entry = Entry(id=uuid.UUID(data[1]),request=loaded_request,meta=loaded_entry_meta,response=loaded_response,cache_key=data[0]) #TODO what should extra be, extra: Mapping[str, Any] = field(default_factory=dict)

        return loaded_entry

    @property
    def is_binary(self) -> bool:
        raise NotImplementedError()


# SyncBaseStorage version
class TiledCache(SyncBaseStorage):
    def __init__(
        self,
        serializer: tp.Optional[TiledSerializer] = None,  # changed from BaseSerializer
        connection: tp.Optional[sqlite3.Connection] = None,
        ttl: tp.Optional[tp.Union[int, float]] = None,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ) -> None:
        # ttl is in seconds, capacity and max_item_size are in bytes
        self._serializer = serializer or TiledSerializer()
        self._ttl = ttl
        self._connection: tp.Optional[sqlite3.Connection] = connection or None
        self._setup_completed: bool = False
        self._lock = SerializableLock()

        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory.
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", platformdirs.user_cache_dir("tiled"))
            )
            # TODO Consider defaulting to a temporary database, with a warning,
            # if TILED_CACHE_DIR points to a networked filesystem. Unless perhaps
            # flock() support can be checked (nfs version, or lock manager, etc).
            filepath = TILED_CACHE_DIR / "http_response_cache.db"
        self._filepath = filepath
        self._capacity = None
        self.capacity = capacity
        self._max_item_size = None
        self.max_item_size = max_item_size
        self._readonly = readonly

    def _setup(self) -> None:
        if not self._setup_completed:
            if not self._connection:
                # The methods in the Cache storage object will not try to write when
                # in readonly mode. For extra safety, we open a readonly connection
                # to the database, so that SQLite itself will prohibit writing.
                database = (
                    f"file:{self._filepath}?ro" if self._readonly else self._filepath
                )
                self._connection = sqlite3.connect(
                    database, uri=self._readonly, check_same_thread=False
                )
            cursor = self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )  # this is getting all the tables I believe
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                # We have an empty database
                self._create_tables()
            elif "tiled_http_response_cache_version" not in tables:
                # We have a non-empty database that we do not recognize.
                raise RuntimeError(
                    f"Database at {self._filepath} is not empty and is not recognized as a Tiled HTTP response cache."
                )
            else:
                # We have a non-empty database that we recognize.
                cursor = self._connection.execute(
                    "SELECT * FROM tiled_http_response_cache_version;"
                )  # I think this is getting everything from inside the table that corresponds to tiled_http_response_cache_version
                (version,) = cursor.fetchone()
                if version != CACHE_DATABASE_SCHEMA_VERSION:
                    # It is likely that this cache database will be very stable,
                    # but if we must make changes we will not bother with migrations.
                    # The cache is highly disposable. Just silently blow it away and start over.
                    Path(self._filepath).unlink()
                    self._connection = sqlite3.connect(
                        self._filepath, check_same_thread=False
                    )
                    self._create_tables()  # I think this is making a new connection and tables if the schema version doesn't match
            cursor.close()
            self._setup_completed = True

    @with_safe_threading
    def _create_tables(self) -> None:
        with closing(
            self._connection.cursor()
        ) as cursor:  # closing is a fancy way of doing .close() but at the beginning. so it would be the same as doing .close() at the end of the block
            cursor.execute(
                """CREATE TABLE responses (
cache_key TEXT PRIMARY KEY,
id TEXT,
status_code INTEGER,
headers JSON,
body BLOB,
is_stream INTEGER,
encoding TEXT,
size INTEGER,
request JSON,
time_created REAL,
time_last_accessed REAL
)"""
            )
            #TODO: removed number_of_uses INTEGER, do we actually need?
            cursor.execute(
                "CREATE TABLE tiled_http_response_cache_version (version INTEGER)"
            )
            cursor.execute(
                "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
                (CACHE_DATABASE_SCHEMA_VERSION,),
            )
            self._connection.commit()

    def __repr__(self):
        module = type(self).__module__
        qualname = type(self).__qualname__
        memaddress = hex(id(self))
        dbfile = str(self.filepath)
        return f"<{module}.{qualname} object at {memaddress} using database {dbfile!r}>"

    def __getstate__(self):
        return (
            self._setup_completed,
            self._lock,
            self._filepath,
            self._capacity,
            self._max_item_size,
            self._readonly,
        )

    def __setstate__(self, state):
        (setup_completed, lock, filepath, capacity, max_item_size, readonly) = state
        self._lock = lock
        self._filepath = filepath
        self._capacity = capacity
        self._max_item_size = max_item_size
        self._readonly = readonly
        if setup_completed:
            self._setup()

    @property
    def filepath(self):
        """Filepath of the SQLite database used for storing cache data"""
        return self._filepath

    @property
    def capacity(self):
        """Max capacity of the cache, in bytes. Includes the response AND request bodies."""
        return self._capacity

    @capacity.setter
    def capacity(self, capacity):
        if capacity < 1:
            raise ValueError("Cache capacity cannot be less than 1 byte")
        elif self.max_item_size and capacity < self.max_item_size:
            raise ValueError("Cache capacity cannot be less than allowed entry size")
        self._capacity = capacity

    @property
    def max_item_size(self):
        """
        Max size of a response body that can be accepted into the cache.
        The size of the request body will be included against this limit.
        """
        return self._max_item_size

    @max_item_size.setter
    def max_item_size(self, max_item_size):
        if max_item_size < 1:
            raise ValueError("Cache entry size cannot be less than 1 byte")
        elif max_item_size > self.capacity:
            raise ValueError("Cache entry size cannot be greater than cache capacity")
        self._max_item_size = max_item_size

    @property
    def readonly(self):
        """If readonly, cache can be read but not updated."""
        return self._readonly

    # TODO transition
    @with_safe_threading
    def _create_entry(self, request: Request, response: Response, key: str, id_: uuid.UUID | None = None) -> Entry:
        """
        Store an entry in the cache.
        TODO fix
        :param key: The key which identifies the entry in the cache
        :type key: str
        :param response: An HTTP response
        :type response: httpcore.Response
        :param request: An HTTP request
        :type request: httpcore.Request
        :param metadata: Additional information about the stored response
        :type metadata: Metadata
        :param response_content: Provide if the response does not yet have content, defaults to None
        :type response_content: tp.Optional[bytes], optional
        :param request_content: Provide if the request does not yet have content, defaults to None
        :type request_content: tp.Optional[bytes], optional

        """
        response_content = response.content.decode("ascii")
        request_content = request.content.decode("ascii")

        if self._connection is None or not self._setup_completed():
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot store new entries in read-only cache")
        incoming_size = measure_entry_size(
            request, response, response_content, request_content
        )
        # TODO: so here's the deal: I think response_content and request_content are necessary and are what
        # makes streaming possible. However, I do not know how to. get these values wihout adding it to the parameter 
        # like what previous _store did. If we add parameters, it won't match Hishel's SyncBaseStorage, so atp idk what
        # the point of using hishel would be. there must be some other way to get these values
        if incoming_size > self.max_item_size:
            logger.debug(
                f"Cache declined entry which is too large: {incoming_size} > {self.max_item_size} (bytes)"
            )
            return
        # Do we need to create a metadata object for our schema?
        entry_meta = EntryMeta(
            created_at=datetime.now().timestamp()
        )
        with closing(self._connection.cursor()) as cursor:
            (total_size,) = cursor.execute("SELECT SUM(size) FROM responses").fetchone()
            total_size = total_size or 0  # If empty, total_size is None
            while (incoming_size + total_size) > self.capacity:
                (cached_key, size) = cursor.execute(
                    """SELECT cache_key, size FROM responses ORDER BY time_last_accessed ASC"""
                ).fetchone()
                cursor.execute(
                    "DELETE FROM responses WHERE cache_key = ?", [cached_key]
                )
                total_size -= size
                #TODO: deleted number_of_uses from below, may need to find a way to put back
            cursor.execute(
                """INSERT OR REPLACE INTO responses(
cache_key,
id,
status_code,
headers,
body,
is_stream,
encoding,
size,
request,
time_created,
time_last_accessed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    key, id_,
                    *self._serializer.dumps(
                        response, request, entry_meta, response_content, request_content
                    ),
                ],
                # handle streaming request?
            )
            self._connection.commit()

    def create_entry(self, request: Request, response: Response, key: str, id_: uuid.UUID | None = None) -> Entry:
        if not self.setup_completed:
            self._setup
        self._create_entry(response, request, key, id_)
        self._remove_expired_caches()

    # TODO transition
    def get_entries(self, key: str) -> tp.List[Entry]:
        """
        Retreive a response from the cache according to the provided key.

        :param key: The key which identifies the entry in the cache
        :type key: str
        :return: An HTTP response and its HTTP request.
        :rtype: tp.Optional[StoredResponse]
        """
        if not self._setup_completed():
            self._setup()
        self._removed_expired_caches()
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                """SELECT
cache_key, id, status_code, headers, body, is_stream, encoding, request, time_created
FROM responses
WHERE cache_key = ?""",
                [
                    key
                ],  # TODO I think we should add cache_key and number_of_uses here in the select, and then we would need to change the indices in the loads function
            )
            row = cursor.fetchone()
            if row is None:
                # TODO: CACHE MISS right? look for hishel logger
                logger.debug(f"Cache miss: {key}") # or info?
                return None
            else:
                #CACHE HIT log
                logger.debug(f"Cache hit: {key}") # or info?

        return self._serializer.loads(row)

    # TODO transition
    @with_safe_threading
    def _remove_entry(self, id: uuid.UUID) -> None:
        """
        Removes the response from the cache.

        :param key: The key which identifies the entry in the cache or an HTTP response
        :type key: Union[str, Response]
        """
        if self._connection is None or not self._setup_completed():
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot remove entries from read-only cache")
        if isinstance(key, Response):
            key = tp.cast(str, key.extensions["cache_metadata"]["cache_key"])
        with closing(self._connection.cursor()) as cursor:
            cursor.execute("DELETE FROM responses WHERE cache_key = ?", [key])
            self._connection.commit()

    # TODO transition
    def remove_entry(self, id: uuid.UUID) -> None:
        if not self._setup_completed:
            self._setup()
        return self._remove_entry(key)

    # TODO transition
    @with_safe_threading
    def update_entry(
        self,
        id: uuid.UUID,
        new_entry: tp.Union[Entry, tp.Callable[[Entry], Entry]],
    ) -> tp.Optional[Entry]:
        """
        Updates the metadata of the stored response.

        :param key: The key which identifies the entry in the cache
        :type key: str
        :param response: An HTTP response
        :type response: httpcore.Response
        :param request: An HTTP request
        :type request: httpcore.Request
        :param metadata: Additional information about the stored response
        :type metadata: Metadata
        :param response_content: Provide if the response does not yet have content, defaults to None
        :type response_content: tp.Optional[bytes], optional
        :param request_content: Provide if the request does not yet have content, defaults to None
        :type request_content: tp.Optional[bytes], optional

        This method was heavily inspired from Hishel's own implementation.
        """
        if self._connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot update entries in read-only cache")
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                "SELECT headers, number_of_uses, time_last_accessed FROM responses WHERE cache_key = ?",
                [key],
            )
            row = cursor.fetchone()
            if row is not None:
                serialized_headers = json.dumps(
                    [
                        (
                            header_key.decode(encoding="ascii"),
                            header_value.decode("ascii"),
                        )
                        for header_key, header_value in response.headers
                    ]
                )
                cursor.execute(
                    "UPDATE responses SET headers = ?, number_of_uses = ?, time_last_accessed = ? WHERE cache_key = ?",
                    [
                        serialized_headers,
                        metadata["number_of_uses"],
                        datetime.now().timestamp(),
                        key,
                    ],
                )
                self._connection.commit()
                return
        return self.store(
            key, response, request, metadata, response_content, request_content
        )

    # TODO transition
    def update_entry(
        self,
        key: str,
        response: Response,
        request: Request,
        metadata: Metadata,
        response_content: tp.Optional[bytes] = None,
        request_content: tp.Optional[bytes] = None,
    ) -> None:
        if self._connection is None or not self._setup_completed:
            self._setup()
        return self._update_entry(
            key, response, request, metadata, response_content, request_content
        )

    @with_safe_threading
    def _remove_expired_caches(self) -> None:
        """Remove all expired entries from the cache."""
        if self._connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot remove entries from read-only cache")
        if self._ttl is None:
            return
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                "DELETE FROM responses WHERE time_created + ? < ?",
                [self._ttl, datetime.now().timestamp()],
            )
            self._connection.commit()

    @with_safe_threading
    def clear(self):
        """Drop all entries from HTTP response cache."""
        if self._connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        with closing(self._connection.cursor()) as cursor:
            cursor.execute("DELETE FROM responses")
            self._connection.commit()

    def size(self):
        """
        Size of response bodies in cache in bytes.
        Includes the size of the corresponding request bodies.
        Does not include the size of headers and other auxiliary info.
        """
        if self._connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with closing(self._connection.cursor()) as cursor:
            (total_size,) = cursor.execute("SELECT SUM(size) FROM responses").fetchone()
        return total_size or 0  # if empty, total_size is None

    def count(self):
        """Number of responses cached."""
        if self._connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with closing(self._connection.cursor()) as cursor:
            (count,) = cursor.execute("SELECT COUNT(*) FROM responses").fetchone()
        return count or 0  # if empty, count is None

    # TODO implement
    def refresh_entry_ttl(self, id: uuid.UUID) -> None:


    def close(self) -> None:
        """Close the cache."""
        if self._connection is not None:
            self._connection.close()




###
import hishel
import httpx

controller = hishel.Controller(key_generator=create_cache_key)
tiled_cache = TiledCache()
transport = hishel.CacheTransport(transport=httpx.HTTPTransport(), storage=tiled_cache)
# This transport is how hishel is used, and it plugs in our TiledCache
###