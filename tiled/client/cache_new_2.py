import enum
import os
import sqlite3
import sys
import threading
import typing as tp
import uuid
from contextlib import closing
from datetime import datetime
from functools import wraps
from hashlib import sha256
from pathlib import Path

import platformdirs
from hishel import Entry, EntryMeta, SyncSqliteStorage
from httpcore import Request, Response

from .logger import logger

# TODO I believe this would stay as 2 since Nate's version wasn't pushed to GitHub and used but double check.
CACHE_DATABASE_SCHEMA_VERSION = 2

# This is currently only used for checking SQlite thread-safety
PY311 = sys.version_info >= (3, 11)


def with_thread_lock(fn):
    """Makes sure the wrapper isn't accessed concurrently."""

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        obj._lock.acquire()
        try:
            result = fn(obj, *args, **kwargs)
        finally:
            obj._lock.release()
        return result

    return wrapper


class ThreadingMode(enum.IntEnum):
    """Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety

    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3

# TODO: delete create_cache_key?
# def create_cache_key(request: Request, body: bytes = b"") -> str:
#     """
#     Generate a Cache key. A Cache key contains the method, url, and request body.
#     :param request: An HTTP request
#     :type request: httpcore.Request
#     :param body: The body of the request. To be included for e.g. POST
#     :type body: tp.Optional[bytes]
#     """
#     # SyncCacheTransport uses SyncCacheProxy which has _get_key_for_request
#     # to make its keys. perhaps we just use that?
#     # I think that's what it automatically does so this doesn't even run to make a key
#     method = request.method.upper().encode()
#     url = request.url
#     hasher = sha256()
#     hasher.update(body)
#     body_hashed = hasher.hexdigest()
#     return f"{method}|{url}|{body_hashed}"


# FIxxxxxx
# TODO, test to see if handling streams right
# i'm very suspicious of this function... but at least it's not returning 0 anymore
# the read() is consuming the stream and then causing a problem with when the stream table
# is trying to be made in the parent


# Idea: To deal with the stream, can check the size as it's saving
# and then if it ever becomes too large, stop writing the stream
# to the streams table and delete the partial entry that hs been
# started to be saved
def measure_entry_size(request, response):
    # httpcore exception that is == httpx.ResponseNotRead()
    # Trace out the way this works for a streaming response
    # Also handle streaming request
    size = 0  # TODO delete

    if hasattr(response, "headers") and "content-length" in response.headers:
        size += int(response.headers["content-length"])

    #TODO: see if this is actually right for request
    if hasattr(request, "headers") and "content-length" in request.headers:
        size += int(request.headers["content-length"])

    return size


class TiledCache(SyncSqliteStorage):
    def __init__(
        self,
        *,
        connection: tp.Optional[sqlite3.Connection] = None,
        ttl: tp.Optional[tp.Union[int, float]] = None,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ) -> None:
        # ttl is in seconds, capacity and max_item_size are in bytes

        self._setup_completed: bool = False

        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory.
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", platformdirs.user_cache_dir("tiled"))
            )
            # if(TILED_CACHE_DIR points to networked file system){
            #     filepath = ":memory:"
            # }
            # it seems complicated to figure out if a file system is networked or not
            # TODO Consider defaulting to a temporary database, with a warning,
            # if TILED_CACHE_DIR points to a networked filesystem. Unless perhaps
            # flock() support can be checked (nfs version, or lock manager, etc).
            # this is for file locking for networked file systems not having file locking. give named in memory cache. how feasible to do that with hishel?
            # how hard to override parameters to set in memory cache with sqlite3
            # else:
            filepath = TILED_CACHE_DIR / "http_response_cache.db"
        self._filepath = filepath
        self._capacity = None
        self._max_item_size = None
        self.capacity = capacity
        self.max_item_size = max_item_size
        self._readonly = readonly  # unique to tiled
        self._owner_thread = threading.current_thread().ident

        super().__init__(connection=connection, database_path=filepath, default_ttl=ttl)

        self._setup()

    def write_safe(self):
        """Check that it is safe to write.

        SQLite is not threadsafe for concurrent _writes_ unless the
        underlying sqlite library was built with thread safety
        enabled. Even still, it may be a good idea to use a thread
        lock (``@with_thread_lock``) to prevent parallel writes.

        """
        is_main_thread = threading.current_thread().ident == self._owner_thread
        sqlite_is_safe = sqlite3.threadsafety == ThreadingMode.SERIALIZED
        return (
            is_main_thread or sqlite_is_safe
        )  # this is just checking if it is safe for multiple threads

    # This may seem redundant because of the _initialized boolean value in the parent, however I think
    # that this is still necessary in case a bug happens where it gets initialized in the parent but not here.
    # If this happens, the tables won't have all the columns it needs and it can be a problem. So, we have to check
    # if our database got initialized here.
    def _setup(self) -> None:
        if not self._setup_completed:
            if not self.connection:
                # The methods in the Cache storage object will not try to write when
                # in readonly mode. For extra safety, we open a readonly connection
                # to the database, so that SQLite itself will prohibit writing.
                database = (
                    f"file:{self._filepath}?mode=ro"
                    if self._readonly
                    else self._filepath
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
                self._initialize_database()

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
                    self._initialize_database()

            cursor.close()
            self._initialized = True
            self._setup_completed = True

    def _initialize_database(self) -> None:
        super()._initialize_database()
        with self._lock, closing(self.connection.cursor()) as cursor:
            # add the additioanl columns here
            # FORMAT:
            # cursor.execute("ALTER TABLE {table_name} ADD COLUMN {variable_name} INTEGER")
            #  Missing from new: body, is_stream(separate table?), encode (perhaps just default to ascii on everything?, size, time_last_accessed)
            # below might be an issue if those columns already exist

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
        elif self._max_item_size and capacity < self._max_item_size:
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

    # _create_entry is being called even during a cache hit which I think is what is
    # causing the count to be 2 instead of 1
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
            return Entry(
                id=id_ or uuid.uuid4(),
                request=request,
                response=response,  # save_stream isn't attached since the parent create_entry is never called so doesn't get streamed
                meta=EntryMeta(created_at=datetime.now().timestamp()),
                cache_key=key.encode("utf-8"),
            )
        if not self.write_safe():
            raise RuntimeError("Write is not safe from another thread")
        with self._lock, closing(self.connection.cursor()) as cursor:
            # Check the size of the request and response
            # SizeFilter
            # REDIS verison has max_stream_size.. don't see for SQL thoguh
            # TODO might have to change name depending on request

            # This is an intial check to see if the response/request are too large to cache as an entry
            request_and_response_size = measure_entry_size(request, response)
            if request_and_response_size > self.max_item_size:
                logger.debug(
                    f"Cache declined entry which is too large: {request_and_response_size} > {self.max_item_size} (bytes)"
                )
                return Entry(
                    id=id_ or uuid.uuid4(),
                    request=request,
                    response=response,  # save_stream isn't attached since the parent create_entry is never called so doesn't get streamed
                    meta=EntryMeta(created_at=datetime.now().timestamp()),
                    cache_key=key.encode("utf-8"),
                )
                # doesn't get commited

            parent_entry = super().create_entry(  # This commits in the parent
                request=request, response=response, key=key, id_=id_
            )  # this should handle the stream table

            # This accumulated_size_state was made into a dict so that both request and response can
            # access it and so we know if the exceed handled already so
            # that logic doesn't keep running as the stream finishes out
            accumulated_size_state = {"size": 0, "exceed_handled": False}
            parent_entry.request.stream = self._check_max_stream_bytes(
                parent_entry.id, parent_entry.request.stream, accumulated_size_state
            )
            parent_entry.response.stream = self._check_max_stream_bytes(
                parent_entry.id, parent_entry.response.stream, accumulated_size_state
            )
            # next(parent_entry.response.stream)

            # So save_stream uses a generator, and then as data is needed it
            # is consumed and saved by the generator, so the streams table isn't
            # complete until this happens.
            # The parent create_entry calls save_stream which makes a wrapper generator,
            # and it sits, waiting until the consumer wants data by calling next()
            # on the generator. So there is nothing in the stream table
            # This is confirmed with next(parent_entry.response.stream)

            # TODO stream_size shouldn't be like this...
            # just raise an exception?
            #  Instead of caching the whole item, use strategies like compression, pagination, chunking, or bypassing the cache entirely for large assets\
            # Pagination idea: Different pages of information?
            # Compression: is it reasonable to compress something so large into 1 byte (or other edge cases)? I feel like this is a stretch
            # Chunking: Splitting it up into multiple entries, but I feel like that wouldn't work for edge cases such as what we're testing here
            # Bypassing: this would be the skipping it I believe.
            # https://medium.com/but-it-works-on-my-machine/caching-101-what-not-to-cache-and-why-9fcd346cd535
            # I think the issue with the exception is anytime it tries to cache and it's too large it'll raise an exception
            # instead of just continuing without it being cached
            # raise ValueError(f"Cache declined entry which is too large: {incoming_size} > {self.max_item_size} (bytes)")
            # same notes as above except for the save_stream thing, TODO look into that make sure it won't be a problem

            # Now that the parent was called, account for the additional table entries.
            # the entries will just be Null to start with since parent didn't set them, so just have to update

            entry = Entry(
                id=parent_entry.id,
                request=parent_entry.request,
                response=parent_entry.response,
                meta=parent_entry.meta,
                cache_key=parent_entry.cache_key,
            )

            cursor.execute(
                "UPDATE entries SET size = ?, time_last_accessed = ? WHERE id = ?",
                (request_and_response_size, datetime.now().timestamp(), entry.id.bytes),
            )  # entry.id.bytes uses the UUID to find the right entry, converting to BLOB

            (total_size,) = cursor.execute(
                "SELECT SUM(size) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
            total_size = total_size or 0  # If empty, total_size is None

            # This is the LRU eviction. if there's not enough space will evict before adding the new one
            while (total_size) > self.capacity:
                (entry_id, size) = cursor.execute(
                    """SELECT id, size FROM entries WHERE deleted_at is NULL ORDER BY time_last_accessed ASC"""
                ).fetchone()
                self.remove_entry(
                    uuid.UUID(bytes=entry_id)
                )  # this is to soft delete in case stream is still being read
                # The uuid.UUID(bytes=) stuff is because hishel expects a UUID and wants to convert it to bytes itself
                # TODO: need to delete from streams?
                total_size -= size

            # Missing from new: body (i don't think we need body 'cause hishel is handling streaming), is_stream(separate table?), encoding (perhaps just default to ascii on everything?, size, time_last_accessed)
            # fine to add them, just need an aditional thing in Tiled to handle them beyond pack and unpack. so --> can keep all
            # ask Nate about the encoding part

            self.connection.commit()

            return entry

    @with_thread_lock
    def create_entry(
        self,
        request: Request,
        response: Response,
        key: str,
        id_: uuid.UUID | None = None,
    ) -> Entry:
        if not self._setup_completed:
            self._setup()
        if not self.get_entries(
            key=key
        ):  # This check is here for the bug of creating multiple entries on a cache hit.
            entry = self._create_entry(request, response, key, id_)
        else:
            return self.get_entries(key=key)[0]
        if (
            entry is not None
        ):  # This is in the event that the entry we are trying to cache is too large and cannot be cached
            self._remove_expired_caches()
        return entry

    # Generator to keep track of how many bytes were streamed to ensure
    # entry remains under the max entry size for the cache.
    # If the entry exceeds, it is deleted from the cache but the stream continues.
    #
    def _check_max_stream_bytes(self, entry_id, stream_iterator, accumulated_size_state):
        for chunk in stream_iterator:
            accumulated_size_state["size"] += len(chunk)
            if not accumulated_size_state["exceed_handled"]:
                # Below deletes the entry when too large
                if accumulated_size_state["size"] > self.max_item_size:
                    accumulated_size_state["exceed_handled"] = True
                    self.remove_entry(entry_id)  # This soft deletes for the entry table
                    logger.debug(
                        f"Cache declined entry which is too large with stream: > {self.max_item_size} (bytes)"
                    )
                else:
                    # should the size in entries include the size of the streamed chunks?
                    with self._lock, closing(self.connection.cursor()) as cursor:
                        cursor.execute(
                            "UPDATE entries SET size = ? WHERE id = ?",
                            (accumulated_size_state["size"], entry_id.bytes),
                        )
                        self.connection.commit()
            # TODO: consider adding LRU eviction in case adding the stream exceeds the max cache size.
            # However, this could be problematic if the stream becomes deleted later because the entry
            # itself is too large, than we are evicting when we didn't need to
            yield chunk

            if accumulated_size_state["exceed_handled"]:
                # This deletes from streams after generator finishes. Does this after
                # generator finishes because chunks are written lazily
                with self._lock, closing(self.connection.cursor()) as cursor:
                    cursor.execute(
                        "DELETE FROM streams WHERE entry_id = ?",
                        (entry_id.bytes,),
                    )
                    self.connection.commit()

    @with_thread_lock
    def get_entries(self, key: str) -> tp.List[Entry]:
        """
        Retreive a response from the cache according to the provided key.

        :param key: The key which identifies the entry in the cache
        :type key: str
        :return: An HTTP response and its HTTP request.
        :rtype: tp.Optional[StoredResponse]
        """
        if not self._setup_completed:
            self._setup()

        parent_entries = super().get_entries(key=key)

        if not parent_entries:
            logger.info(f"Cache miss: {key}")
            return []
        else:
            logger.info(f"Cache hit: {key}")

        # TODO: put a big chunk of data to see if it is significantly improved perfcounter

        # This is here to update time_last_accessed for the sake of the LRU eviction
        # need to update it in the Entry list AND in the table

        with self._lock, closing(self.connection.cursor()) as cursor:
            entries = []
            for entry in parent_entries:
                updated_entry = Entry(
                    id=entry.id,
                    request=entry.request,
                    meta=entry.meta,
                    response=entry.response,
                    cache_key=entry.cache_key,
                )
                entries.append(updated_entry)
                # above deals with the returned entries list, below deals with the table
                if not self.readonly and self.write_safe():
                    cursor.execute(
                        "UPDATE entries SET time_last_accessed = ? WHERE id = ?",
                        (datetime.now().timestamp(), entry.id.bytes),
                    )
                    self.connection.commit()
            return entries

    # Deleted _remove_entry and remove_entry because the parent already does it.
    # HOWEVER, may need to add it back for the sake of checking if the
    # cache was setup, depends on what we want to do with that. I think ask Nate for thoughts

    # perhaps change to call _create_entry?
    def update_entry(
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
        if not self.readonly:
            # TODO: do we need to do the size check here again?
            completed_entry = super().update_entry(id=id, new_pair=new_entry)
            # note for understanding, in the parent update_entry, the "data" is the Entry object
            with self._lock:
                connection = (
                    self._ensure_connection()
                )  # Note: I think there would be a bug here with if we were to remove the setup check because then the database wouldn't be initialized, so it will only initialiize in the parent and then not add the additional columns that we do here
                cursor = connection.cursor()
                cursor.execute(
                    "UPDATE entries SET time_last_accessed = ? WHERE id = ?",
                    (
                        datetime.now().timestamp(),
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
            return
        if self.default_ttl is None:
            return
        with self._lock, closing(self.connection.cursor()) as cursor:
            entry_ids = cursor.execute(
                "SELECT id FROM entries WHERE created_at + ? < ?",
                [self.default_ttl, datetime.now().timestamp()],
            ).fetchall()
            for (entry_id,) in entry_ids:
                self.remove_entry(uuid.UUID(bytes=entry_id))
                # TODO: should this be soft deleted for streams?
                cursor.execute(
                    "DELETE FROM streams WHERE entry_id = ?",
                    [entry_id],
                )

    @with_thread_lock
    def clear(self):
        """Drop all entries from HTTP response cache."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        if not self.write_safe():
            raise RuntimeError(
                "Cannot clear cache from a different thread than the one it was created on"
            )
        with self._lock, closing(self.connection.cursor()) as cursor:
            cursor.execute("DELETE FROM streams")
            cursor.execute("DELETE FROM entries") #TODO: should this be soft deleted?
            self.connection.commit()

    def size(self):
        """
        Size of response bodies in cache in bytes.
        Includes the size of the corresponding request bodies.
        Does not include the size of headers and other auxiliary info.
        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with self._lock, closing(self.connection.cursor()) as cursor:
            (total_size,) = cursor.execute(
                "SELECT SUM(size) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
        return total_size or 0  # if empty, total_size is None

    def count(self):
        """Number of responses cached."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with self._lock, closing(self.connection.cursor()) as cursor:
            (count,) = cursor.execute(
                "SELECT COUNT(*) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
        return count or 0  # if empty, count is None
