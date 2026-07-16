"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
import typing as tp

import httpx
from hishel.httpx import SyncCacheTransport
from hishel import CacheOptions, SpecificationPolicy

from .cache_new_2 import TiledCache
from .logger import collect_request, collect_response, log_request, log_response, logger
from .utils import TiledResponse


class TiledTransport(httpx.BaseTransport):
    # TODO update message up here
    """Custom transport, implementing caching and custom compression encodings.

    Args:
        transport (optional): an existing httpx transport, if no transport
            is given, defaults to an httpx.HTTPTransport with default args.
        cache (optional): cache to use with this transport, defaults to
            httpx_cache.DictCache
        cacheable_methods: methods that are allowed to be cached, defaults to ['GET']
        cacheable_status_codes: status codes that are allowed to be cached,
            defaults to: (200, 203, 300, 301, 308)
    """

    def __init__(
        self,
        *,
        transport: tp.Optional[httpx.BaseTransport] = None,
        cache: tp.Optional[TiledCache] = None,
        limits: tp.Optional[httpx.Limits] = None,
        cacheable_methods: tp.Tuple[str, ...] = ("GET",),
        cacheable_status_codes: tp.Tuple[int, ...] = (
            httpx.codes.OK,
            httpx.codes.NON_AUTHORITATIVE_INFORMATION,
            httpx.codes.MULTIPLE_CHOICES,
            httpx.codes.MOVED_PERMANENTLY,
            httpx.codes.PERMANENT_REDIRECT,
        ),
        always_cache: bool = False,
    ):
        self.cacheable_methods = cacheable_methods
        if transport is not None:
            self.transport = transport
        elif limits is not None:
            self.transport = httpx.HTTPTransport(limits=limits)
        else:
            self.transport = httpx.HTTPTransport()
        self.cache = cache #This sets the cache from the cache.setter below

    # The two functions below are also in context, but this fixes the bugs from pytests
    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        # this is in case the cache changes to prevent a stale transport by reapplying the wrapper so that SyncCacheTransport doesn't continue to point at the old storage
        self._cache = cache
        if cache is None:
            self._active_transport = self.transport
        else: 
            self._active_transport = SyncCacheTransport( #wrapper so we can use the Hishel transport. Handles writing etc for us
                policy=SpecificationPolicy(cache_options=CacheOptions(supported_methods=self.cacheable_methods)),  #TODO: do we need to account for cacheable_status_codes and always_cache
                next_transport=self.transport,
                storage=cache,
            )

    def close(self) -> None:
        self.transport.close()
        if self.cache is not None:
            self.cache.close()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        
        # Call original transport
        if __debug__:
            log_request(request)
            collect_request(request)
        response = self._active_transport.handle_request(request) 
        response.__class__ = TiledResponse
        response.request = request
        if __debug__:
            # Log the actual server traffic, not the cached response.
            log_response(response)
            # But, below _collect_ the response with the content in it.
        if __debug__:
            collect_response(response)
        return response


# For when we implement an Async client
#
# class AsyncCacheControlTransport(httpx.AsyncBaseTransport):
#     """Async CacheControl transport for httpx_cache.
#
#     Args:
#         transport (optional): an existing httpx async-transport, if no transport
#             is given, defaults to an httpx.AsyncHTTPTransport with default args.
#         cache (optional): cache to use with this transport, defaults to
#             httpx_cache.DictCache
#         cacheable_methods: methods that are allowed to be cached, defaults to ['GET']
#         cacheable_status_codes: status codes that are allowed to be cached,
#             defaults to: (200, 203, 300, 301, 308)
#     """
#
#     def __init__(
#         self,
#         *,
#         transport: tp.Optional[httpx.AsyncBaseTransport] = None,
#         cache: tp.Optional[BaseCache] = None,
#         cacheable_methods: tp.Tuple[str, ...] = ("GET",),
#         cacheable_status_codes: tp.Tuple[int, ...] = (200, 203, 300, 301, 308),
#         always_cache: bool = False,
#     ):
#         self.controller = CacheControl(
#             cacheable_methods=cacheable_methods,
#             cacheable_status_codes=cacheable_status_codes,
#             always_cache=always_cache,
#         )
#         self.transport = transport or httpx.AsyncHTTPTransport()
#         self.cache = cache or DictCache()
#
#     async def aclose(self) -> None:
#         await self.cache.aclose()
#         await self.transport.aclose()
#
#     async def handle_async_request(self, request: httpx.Request) -> TiledResponse:
#         # check if request is cacheable
#         if self.controller.is_request_cacheable(request):
#             logger.debug(f"Checking cache for: %s", request)
#             cached_response = await self.cache.aget(request)
#             if cached_response is not None:
#                 logger.debug(f"Found cached response for: %s", request)
#                 if self.controller.is_response_fresh(
#                     request=request, response=cached_response
#                 ):
#                     setattr(cached_response, "from_cache", True)
#                     return cached_response
#                 else:
#                     logger.debug(f"Cached response is stale, deleting: %s", request)
#                     await self.cache.adelete(request)
#
#         # Request is not in cache, call original transport
#         response = await self.transport.handle_async_request(request)
#
#         if self.controller.is_response_cacheable(request=request, response=response):
#             if hasattr(response, "_content"):
#                 logger.debug(f"Caching response for: %s", request)
#                 await self.cache.aset(request=request, response=response)
#             else:
#                 # Wrap the response with cache callback:
#                 async def _callback(content: bytes) -> None:
#                     logger.debug(f"Caching response for: %s", request)
#                     await self.cache.aset(
#                         request=request, response=response, content=content
#                     )
#
#                 response.stream = ByteStreamWrapper(
#                     stream=response.stream, callback=_callback  # type: ignore
#                 )
#         setattr(response, "from_cache", False)
#         return response
