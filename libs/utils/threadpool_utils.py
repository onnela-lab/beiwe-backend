from collections.abc import Callable, Iterable, Generator
from multiprocessing.pool import ThreadPool
from typing import Any, TypeVar


T = TypeVar("T")


def s3_op_threadcount() -> int:
    """ Threading is mostly used for s3, which includes network, compression/decompression, and
    encryption/decription.  For now this value is set to 6 based on a hunch. For a while we had a
    thread count of 4, then we compressed files, which reduced them on average to 1/5th. On more
    modern AWS servers this is probably ... fine. """
    return 8


def s3_op_threadpool() -> ThreadPool:
    return ThreadPool(s3_op_threadcount())


def s3_op_threaded_iterate(
    func: Callable[..., T],
    iterable: Iterable[Any],
    *static_args: Any,
    **static_kwargs: Any,
) -> list[T]:
    """ Simple wrapper around threadpool.imap_unordered for the purpose of uploading/downloading
    from s3 in parallel. """
    
    pool = s3_op_threadpool()
    
    try:
        return list(
            pool.imap_unordered(
                lambda iterated: func(iterated, *static_args, **static_kwargs), iterable
            )
        )
    finally:
        pool.close()
        pool.join()
        pool.terminate()


def drain_in_reverse(lst: list[T]) -> Generator[T, None, None]:
    """ Drains a list, this is useful for memory management where order doesn't matter.  Memory
    deallocates as items are popped - to a point; this is Python. """
    
    while lst:
        yield lst.pop()
