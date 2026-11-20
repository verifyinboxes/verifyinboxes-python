"""Reading a list's results page after page.

A page holds at most the rows asked for, and may hold fewer, even none, before the last one: the iterators follow
`next_cursor` until it is `None`, whatever a page holds, so every row comes once and in order. A page that fails is
asked again with the same cursor (the client's retries). An API that answers the cursor it was just sent would make
this go round forever, or repeat rows: that is an error.
"""
from __future__ import annotations

from typing import AsyncIterator, Awaitable, Callable, Iterator, Optional

from ._exceptions import VerifyInboxesError
from ._models import ResultRow, ResultsPage

__all__ = ["iter_results", "aiter_results"]

FetchPage = Callable[[Optional[str]], ResultsPage]
AsyncFetchPage = Callable[[Optional[str]], Awaitable[ResultsPage]]


def _same_cursor(job_id: str, cursor: Optional[str]) -> VerifyInboxesError:
    return VerifyInboxesError(
        f"the API returned the same cursor twice ({cursor!r}) while listing the results of {job_id}", job_ids=[job_id])


def iter_results(job_id: str, fetch: FetchPage) -> Iterator[ResultRow]:
    """Every row, from the first page (`fetch(None)`) to the one whose `next_cursor` is `None`."""
    cursor: Optional[str] = None
    while True:
        page = fetch(cursor)
        yield from page.data
        if page.next_cursor is None:
            return
        if page.next_cursor == cursor:
            raise _same_cursor(job_id, cursor)
        cursor = page.next_cursor


async def aiter_results(job_id: str, fetch: AsyncFetchPage) -> AsyncIterator[ResultRow]:
    """`iter_results`, for `async for`."""
    cursor: Optional[str] = None
    while True:
        page = await fetch(cursor)
        for row in page.data:
            yield row
        if page.next_cursor is None:
            return
        if page.next_cursor == cursor:
            raise _same_cursor(job_id, cursor)
        cursor = page.next_cursor
