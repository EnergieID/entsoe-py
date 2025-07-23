import logging
from functools import wraps
from socket import gaierror
from time import sleep
from http.client import RemoteDisconnected
import pandas as pd
import requests

from .exceptions import NoMatchingDataError, PaginationError
from .misc import day_blocks, year_blocks

logger = logging.getLogger(__name__)


def retry(func):
    """Catches connection errors, waits and retries"""

    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            # Apart from common (ConnectionError and gaierror) errors, in certain
            # cases (e.g. with scheduled commercial exchanges), the connection with
            # ENTSO-e's can break with a RemoteDisconnected exception
            except (requests.ConnectionError, gaierror, RemoteDisconnected) as e:
                error = e
                logger.warning(
                    "Connection Error, "
                    f"retrying in {self.retry_delay} seconds"
                )
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error

    return retry_wrapper


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results"""

    @wraps(func)
    def pagination_wrapper(*args, start, end, **kwargs):
        try:
            df = func(*args, start=start, end=end, **kwargs)
        except PaginationError:
            pivot = start + (end - start) / 2
            df1 = pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            df2 = pagination_wrapper(*args, start=pivot, end=end, **kwargs)
            df = pd.concat([df1, df2])
        return df

    return pagination_wrapper


def documents_limited(n):
    def decorator(func):
        """Deals with calls where you cannot query more than n documents at a
        time, by offsetting per n documents"""

        @wraps(func)
        def documents_wrapper(*args, **kwargs):
            frames = []
            for offset in range(0, 4800 + n, n):
                try:
                    frame = func(*args, offset=offset, **kwargs)
                    frames.append(frame)
                except NoMatchingDataError:
                    logger.debug(f"NoMatchingDataError: for offset {offset}")
                    break

            if len(frames) == 0:
                # All the data returned are void
                raise NoMatchingDataError

            df = pd.concat(
                [frame for frame in frames if not frame.empty and not frame.isna().all().all()],
                sort=True)
            if func.__name__ != '_query_unavailability':
                # For same indices pick last valid value
                if df.index.has_duplicates:
                    df = df.groupby(df.index).agg(deduplicate_documents_limited)
            return df
        return documents_wrapper
    return decorator


def deduplicate_documents_limited(group):
    if group.shape[0] == 1:
        return group
    else:
        return group.ffill().iloc[[-1]]


def year_limited(func):
    """Deals with calls where you cannot query more than a year,
    by splitting the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, start=None, end=None, **kwargs):
        if start is None or end is None:
            raise Exception(
                'Please specify the start and end date explicity with'
                'start=<date> when calling this function'
            )
        if (
            not isinstance(start, pd.Timestamp)
            or not isinstance(end, pd.Timestamp)
        ):
            raise Exception(
                'Please use a timezoned pandas object for start and end'
            )
        if start.tzinfo is None or end.tzinfo is None:
            raise Exception(
                'Please use a timezoned pandas object for start and end'
            )

        blocks = year_blocks(start, end)
        frames = []
        is_first_frame = True  # Assumes blocks are sorted
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
                if func.__name__ != '_query_unavailability' and isinstance(frame.index, pd.DatetimeIndex):
                    # Due to partial matching func may return data indexed by
                    # timestamps outside _start and _end. In order to avoid
                    # (unintentionally) repeating records, frames are truncated to
                    # left-open intervals (or closed interval in the case of the
                    # earliest block).
                    #
                    # If there are repeating records in a single frame (e.g. due
                    # to corrections) then the result will also have them.
                    if is_first_frame:
                        interval_mask = frame.index <= _end
                    else:
                        interval_mask = (
                            (frame.index <= _end)
                            & (frame.index > _start)
                        )
                    frame = frame.loc[interval_mask]
            except NoMatchingDataError:
                logger.debug(
                    f"NoMatchingDataError: between {_start} and {_end}"
                )
                frame = None
            frames.append(frame)
            is_first_frame = False

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames, sort=True)
        return df

    return year_wrapper


def day_limited(func):
    """Deals with calls where you cannot query more than a year,
    by splitting the call up in blocks per year"""

    @wraps(func)
    def day_wrapper(*args, start, end, **kwargs):
        blocks = day_blocks(start, end)
        frames = []
        for _start, _end in blocks:
            try:
                frame = func(*args, start=_start, end=_end, **kwargs)
            except NoMatchingDataError:
                logger.debug(
                    f"NoMatchingDataError: between {_start} and {_end}"
                )
                frame = None
            frames.append(frame)

        if sum([f is None for f in frames]) == len(frames):
            # All the data returned are void
            raise NoMatchingDataError

        df = pd.concat(frames)
        return df

    return day_wrapper
