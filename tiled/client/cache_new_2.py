import enum
import os
import sqlite3
import sys
import typing as tp
import uuid
from contextlib import closing
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import platformdirs
from hishel import (  # instead of BaseStorage there is not SyncBaseStorage and AsyncBaseStorage. There is no BaseSerializer
    Entry,
    SyncSqliteStorage,
)
from httpcore import Request, Response

from .logger import logger

CACHE_DATABASE_SCHEMA_VERSION = 2

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


def measure_entry_size(request, response, stream_size=0):
    # httpcore exception that is == httpx.ResponseNotRead()
    # Trace out the way this works for a streaming response
    # Also handle streaming request
    if hasattr(response, "_content"):
        size = len(response.content)
    elif stream_size is None:
        raise Exception
    else:
        size = stream_size

    if hasattr(request, "_content"):
        size += len(request.content)
    elif stream_size is None:
        raise Exception
    else:
        size += stream_size
    return size


class ThreadingMode(enum.IntEnum):
    """
    Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety
    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3


class TiledCache(SyncSqliteStorage):
    def __init__(
        self,
        connection: tp.Optional[sqlite3.Connection] = None,
        ttl: tp.Optional[tp.Union[int, float]] = None,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ) -> None:
        # ttl is in seconds, capacity and max_item_size are in bytes
        # TODO: removed self._lock since parent has its own, but different type of lock: SerializableLock vs. RLock

        self._setup_completed: bool = False

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
        self._readonly = readonly  # unique to tiled

        super().__init__(
            connection=connection, database_path=filepath, default_ttl=ttl
        )  # TODO: what to do about refresh_ttl_on_access

        self._setup()

    def _setup(self) -> None:
        if not self._setup_completed:
            if not self.connection:
                # The methods in the Cache storage object will not try to write when
                # in readonly mode. For extra safety, we open a readonly connection
                # to the database, so that SQLite itself will prohibit writing.
                database = (
                    f"file:{self._filepath}?ro" if self._readonly else self._filepath
                )
                self.connection = sqlite3.connect(
                    database, uri=self._readonly, check_same_thread=False
                )
            cursor = self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                # We have an empty database
                self._ensure_connection()  # self._ensure_connection will automatically initialize databse once ensuring it is connected
            elif "tiled_http_response_cache_version" not in tables:
                # We have a non-empty database that we do not recognize.
                raise RuntimeError(
                    f"Database at {self._filepath} is not empty and is not recognized as a Tiled HTTP response cache."
                )
            else:
                # We have a non-empty database that we recognize.
                cursor = self.connection.execute(
                    "SELECT * FROM tiled_http_response_cache_version;"
                )
                (version,) = cursor.fetchone()
                if version != CACHE_DATABASE_SCHEMA_VERSION:
                    # It is likely that this cache database will be very stable,
                    # but if we must make changes we will not bother with migrations.
                    # The cache is highly disposable. Just silently blow it away and start over.
                    Path(self._filepath).unlink()
                    self.connection = sqlite3.connect(
                        self._filepath, check_same_thread=False
                    )
                    self._ensure_connection()
            cursor.close()
            self._setup_completed = True  # but parent also has self._initialized to chceck so todo find a way to use that

    def _initialize_database(self) -> None:
        super()._initialize_database()
        with closing(self.connection.cursor()) as cursor:
            # add the additioanl columns here
            # FORMAT:
            # cursor.execute("ALTER TABLE {table_name} ADD COLUMN {variable_name} INTEGER")
            #  Missing from new: body, is_stream(separate table?), encode (perhaps just default to ascii on everything?, size, time_last_accessed)
            # below might be an issue if those columns already exist
            cursor.execute("ALTER TABLE entries ADD COLUMN encoding TEXT")
            cursor.execute("ALTER TABLE entries ADD COLUMN size INTEGER")
            cursor.execute("ALTER TABLE entries ADD COLUMN time_last_accessed INTEGER")

            # The two below tables were in the previous cache version.
            cursor.execute(
                "CREATE TABLE tiled_http_response_cache_version (version INTEGER)"
            )
            cursor.execute(
                "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
                (CACHE_DATABASE_SCHEMA_VERSION,),
            )

            self.connection.commit()

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

    def _create_entry(
        self,
        request: Request,
        response: Response,
        key: str,
        id_: uuid.UUID | None = None,
    ) -> Entry:
        """
        Store an entry in the cache.

        :param request: An HTTP request
        :type request: httpcore.Request
        :param response: An HTTP response
        :type response: httpcore.Response
        :param key: The key which identifies the entry in the cache
        :type key: str
        :param id_: The UUID identifying the entry
        :type id_: UUID

        """

        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot store new entries in read-only cache")

        # TODO: this might be an iissue because it begins to make the table before checking the size
        parent_entry = super().create_entry(
            request=request, response=response, key=key, id_=id_
        )

        with closing(self.connection.cursor()) as cursor:
            (stream_size,) = cursor.execute(
                "SELECT SUM(LENGTH(chunk_data)) FROM streams WHERE entry_id = ?",
                (parent_entry.id.bytes,),
            ).fetchone()  # adds all the lengths together in bytes from blob
            stream_size = stream_size or 0

            incoming_size = measure_entry_size(request, response, stream_size)

            if incoming_size > self.max_item_size:
                super().remove_entry(parent_entry.id)
                logger.debug(
                    f"Cache declined entry which is too large: {incoming_size} > {self.max_item_size} (bytes)"
                )
                return

            # Now that the parent was called, account for the additional table entries.
            # We can modify the Entry object to have the other data values stored in "extra" so then at least
            # the Entry object would have that information, however, so at least its there if we want to access it through that
            # we would also need to updated the actual table in SQL though

            (total_size,) = cursor.execute("SELECT SUM(size) FROM entries").fetchone()
            total_size = total_size or 0  # If empty, total_size is None

            # This is the LRU eviction. if there's not enough space will evict before adding the new one
            while (incoming_size + total_size) > self.capacity:
                (entry_id, size) = cursor.execute(
                    """SELECT id, size FROM entries ORDER BY time_last_accessed ASC"""
                ).fetchone()
                cursor.execute("DELETE FROM entries WHERE id = ?", [entry_id])
                total_size -= size

            extra = {
                "encoding": "ascii",
                "size": incoming_size,
                "time_last_accessed": datetime.now().timestamp(),
            }
            entry = Entry(
                id=parent_entry.id,
                request=parent_entry.request,
                response=parent_entry.response,
                meta=parent_entry.meta,
                cache_key=parent_entry.cache_key,
                extra=extra,
            )
            # TODO: deleted number_of_uses from below, may need to find a way to put back
            #   TODO  Missing from new: body (i don't think we need body 'cause hishel is handling streaming), is_stream(separate table?), encoding (perhaps just default to ascii on everything?, size, time_last_accessed)
            # fine to add them, just need an aditional thing in Tiled to hangle them beyond pack and unpack. so --> can keep all

            # the entries will just be Null to start with since parent didn't set them, so just have to update
            cursor.execute(
                "UPDATE entries SET encoding = ?, size = ?, time_last_accessed = ? WHERE id = ?",
                ("ascii", incoming_size, datetime.now().timestamp(), entry.id.bytes),
            )  # #entry.id.bytes uses the UUID to find the right entry, converting to BLOB

            # TODO: do we also need to deal with the stream table here?
            self.connection.commit()
            return entry

    def create_entry(
        self,
        request: Request,
        response: Response,
        key: str,
        id_: uuid.UUID | None = None,
    ) -> Entry:
        if not self._setup_completed:
            self._setup()
        entry = self._create_entry(request, response, key, id_)
        self._remove_expired_caches()
        return entry

    def get_entries(self, key: str) -> tp.List[Entry]:
        """
        Retreive a response from the cache according to the provided key.

        :param key: The key which identifies the entry in the cache
        :type key: str
        :return: An HTTP response and its HTTP request.
        :rtype: tp.Optional[StoredResponse]
        """
        # TODO: see if hishel has its own cache miss and hit loggers
        # TODO like the other functions, may need to consider the setup aspect
        if not self._setup_completed:
            self._setup()

        parent_entries = super().get_entries(key=key)

        if not parent_entries:
            logger.debug(f"Cache miss: {key}")  # or info?
            return []  # TODO: is this the best thing to return?
        else:
            logger.debug(f"Cache hit: {key}")  # or info?

        # This is here to update time_last_accessed for the sake of the LRU eviction
        # need to update it in the Entry list AND in the table
        with closing(self.connection.cursor()) as cursor:
            entries = []
            for entry in parent_entries:
                extra = {
                    "encoding": entry.extra["encoding"],
                    "size": entry.extra["size"],
                    "time_last_accessed": datetime.now().timestamp(),
                }
                updated_entry = Entry(id=entry.id, request=entry.request, meta=entry.meta, response=entry.response, cache_key=entry.cache_key, extra=extra)
                entries.append(updated_entry)
                # above deals with the returned entries list, below deals with the table
                cursor.execute(
                    "UPDATE entries SET time_last_accessed = ? WHERE id = ?",
                    (datetime.now().timestamp(), entry.id.bytes),
                )
                self.connection.commit()
            return entries

    # Deleted _remove_entry and remove_entry because the parent already does it.
    # TODO HOWEVER, may need to add it back for the sake of checking if the
    # cache was setup, depends on what we want to do with that.

    # need to call the parent and then also update the extra table fields
    # perhaps change to call _create_entry?
    def _update_entry(
        self,
        id: uuid.UUID,
        new_entry: tp.Union[Entry, tp.Callable[[Entry], Entry]],
    ) -> tp.Optional[Entry]:
        """
        Updates the Entry of the stored data.

        :param id: The UUID which identifies the entry in the cache
        :type id: UUID
        :param new_entry: The new Entry that we will be updating to.
        :type new_entry: tp.Union[Entry, tp.Callable[[Entry], Entry]]

        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot update entries in read-only cache")

        completed_entry = super().update_entry(id=id, new_entry=new_entry)
        # note for understanding, in the parent update_entry, the "data" is the Entry object

        # Since we put in the new "data" in the parent update_entry, we might need to just take it
        # out again so that the "extras" field is correct. Actually, the entry object is being passed as a parameter,
        # so we might not need to deal with that and can assume it has the proper values when it is passed in. I think that
        # is the best bet.
        # That being said, I think we should grab what is in the "extra" of the completed_entry
        # and update our table with that
        with self._lock:
            connection = self._ensure_connection()
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE entries SET encoding = ?, size = ?, time_last_accessed = ? WHERE id = ?",
                (
                    completed_entry.extra["encoding"],
                    completed_entry.extra["size"],
                    completed_entry.extra["time_last_accessed"],
                    id.bytes,
                ),
            )
            connection.commit()

            return completed_entry

    # deleted update_entry here. same potential issue with setup as mentioned for previous functions

    def _remove_expired_caches(self) -> None:
        """Remove all expired entries from the cache."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot remove entries from read-only cache")
        if self.default_ttl is None:
            return
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                "DELETE FROM entries WHERE time_created + ? < ?",
                [self.default_ttl, datetime.now().timestamp()],
            )
            self.connection.commit()

    # leave as is?
    def clear(self):
        """Drop all entries from HTTP response cache."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("DELETE FROM entries")
            self.connection.commit()

    # leave as is?
    def size(self):
        """
        Size of response bodies in cache in bytes.
        Includes the size of the corresponding request bodies.
        Does not include the size of headers and other auxiliary info.
        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with closing(self.connection.cursor()) as cursor:
            (total_size,) = cursor.execute("SELECT SUM(size) FROM entries").fetchone()
        return total_size or 0  # if empty, total_size is None

    # leave as is?
    def count(self):
        """Number of responses cached."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with closing(self.connection.cursor()) as cursor:
            (count,) = cursor.execute("SELECT COUNT(*) FROM entries").fetchone()
        return count or 0  # if empty, count is None


###
# import hishel
# import httpx

# controller = hishel.Controller(key_generator=create_cache_key)
# tiled_cache = TiledCache()
# transport = hishel.CacheTransport(transport=httpx.HTTPTransport(), storage=tiled_cache)
# This transport is how hishel is used, and it plugs in our TiledCache
###
