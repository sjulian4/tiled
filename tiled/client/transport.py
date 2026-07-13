"""
Adapted from https://raw.githubusercontent.com/obendidi/httpx-cache/main/httpx_cache/transport.py
in accordance with its BSD-3 license
"""
from hishel.httpx import SyncCacheTransport

import typing as tp

import httpx

from .cache_new_2 import TiledCache, create_cache_key
from .cache_control import ByteStreamWrapper, CacheControl
from .logger import collect_request, collect_response, log_request, log_response, logger
from .utils import TiledResponse


# class TiledTransport(httpx.BaseTransport):
#     """Custom transport, implementing caching and custom compression encodings.

#     Args:
#         transport (optional): an existing httpx transport, if no transport
#             is given, defaults to an httpx.HTTPTransport with default args.
#         cache (optional): cache to use with this transport, defaults to
#             httpx_cache.DictCache
#         cacheable_methods: methods that are allowed to be cached, defaults to ['GET']
#         cacheable_status_codes: status codes that are allowed to be cached,
#             defaults to: (200, 203, 300, 301, 308)
#     """

#     def __init__(self, transport:httpx.BaseTransport, cache):
#         self.transport = transport
#         self.cache = cache

#     def handle_request(self, request: httpx.Request) -> httpx.Response:
#         response = self.transport.handle_request(request)
#         response.__class__ = TiledResponse
#         response.request = request
#         if __debug__:
#             # Log the actual server traffic, not the cached response.
#             log_response(response)
#         collect_request(request)
#         collect_response(response)
#         return response

class TiledTransport(httpx.BaseTransport):
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
        self.controller = CacheControl(
            cacheable_methods=cacheable_methods,
            cacheable_status_codes=cacheable_status_codes,
            always_cache=always_cache,
        )
        if transport is not None:
            self.transport = transport
        elif limits is not None:
            self.transport = httpx.HTTPTransport(limits=limits)
        else:
            self.transport = httpx.HTTPTransport()
        self.cache = cache
    
    # 
    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cache = cache
        if cache is None:
            self._inner = self.transport
        else:
            self._inner = SyncCacheTransport(
                next_transport=self.transport,
                storage=cache,
            )
    # 

    def close(self) -> None:
        self.transport.close()
        if self.cache is not None:
            self.cache.close()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # check if request is cacheable
        if (self.cache is not None) and self.controller.is_request_cacheable(request):
            if __debug__:
                logger.debug("Checking cache for: %s", request)
            key = create_cache_key(request)
            cached_response = self.cache.get_entries(key=key)
            # print("cached_response: ", cached_response)
            if cached_response is not None and cached_response != []:
                if self.controller.is_response_fresh(
                    request=request, response=cached_response[0].response
                ):
                    if not self.controller.needs_revalidation(
                        request=request, response=cached_response[0].response
                    ):
                        if __debug__:
                            logger.debug("Using cached response for: %s", request)
                            log_request(request)
                            collect_request(request)
                        return cached_response
                    if __debug__:
                        logger.debug("Revalidating cached response for: %s", request)
                    request.headers["If-None-Match"] = cached_response[0].response.headers["ETag"]
                else:
                    if __debug__:
                        logger.debug("Cached response is stale, deleting: %s", request)
                    self.cache.delete(request)
            else:
                if __debug__:
                    logger.debug(
                        "No valid cached response found in cache for: %s", request
                    )

        # Call original transport
        if __debug__:
            log_request(request)
            collect_request(request)
        response = self._inner.handle_request(request)
        # response = self.transport.handle_request(request)
        response.__class__ = TiledResponse
        response.request = request
        if __debug__:
            # Log the actual server traffic, not the cached response.
            log_response(response)
            # But, below _collect_ the response with the content in it.

        if self.cache is not None:
            if response.status_code == httpx.codes.NOT_MODIFIED:
                if __debug__:
                    logger.debug(
                        "Server validated as fresh cached entry for: %s", request
                    )
                    collect_response(cached_response)
                return cached_response

            if self.controller.is_response_cacheable(
                request=request, response=response
            ):
                if self.cache.readonly:
                    if __debug__:
                        logger.debug("Cache is read-only; will not store")
                # elif not self.cache.write_safe():
                #     if __debug__:
                #         logger.debug(
                #             "Cannot write to cache from another thread; will not store"
                #         )
                # else:
                #     if hasattr(response, "_content"): #TODO: what is this trying to check?
                #         # like what does it mean if the response has a _content attribute?
                #         # (in the old version)
                #         is_stored = self.cache.create_entry(request=request, response=response, key=key)
                #         if __debug__:
                #             if is_stored:
                #                 logger.debug("Caching response for: %s", request)
                #             else:
                #                 logger.debug(
                #                     "Declined to store large response for: %s", request
                #                 )
                #     else:
                #         # Wrap the response with cache callback:
                #         def _callback(content: bytes) -> None: #TODO: what is the point of this?
                #             is_stored = self.cache.create_entry(
                #                 request=request, response=response, key=key, #TODO: need an ID?
                #             )
                #             if __debug__:
                #                 if is_stored:
                #                     logger.debug("Caching response for: %s", request)
                #                 else:
                #                     logger.debug(
                #                         "Declined to store large response for: %s",
                #                         request,
                #                     )

                #         response.stream = ByteStreamWrapper(
                #             stream=response.stream, callback=_callback  # type: ignore
                #         )
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
