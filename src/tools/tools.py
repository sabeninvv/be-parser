from typing import Union
from time import sleep
from asyncio import sleep as aio_sleep
from functools import wraps
from logging import Logger
import base64


def encode_by_b64(row: str) -> str:
    return base64.urlsafe_b64encode(row.encode("UTF-8")).decode("UTF-8")


def decode_by_b64(encode_row: str) -> str:
    return base64.b64decode(encode_row).decode("UTF-8")


def decoding_bytes2str(data):
    if isinstance(data, bytes):
        return data.decode()
    if isinstance(data, dict):
        return dict(map(decoding_bytes2str, data.items()))
    if isinstance(data, tuple):
        return tuple(map(decoding_bytes2str, data))
    if isinstance(data, list):
        return list(map(decoding_bytes2str, data))
    if isinstance(data, set):
        return set(map(decoding_bytes2str, data))
    return data


def retry(logger: Logger, num_tries: Union[int, float] = 3, idle_time_sec: float = .1):
    def wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            num_try, exception = 0, None
            while num_try <= num_tries:
                try:
                    return fn(*args, **kwargs)
                except Exception as ex:
                    logger.info(f"{wrapper.__str__()} - Retry: {ex}")
                    exception = ex
                    num_try += 1
                sleep(idle_time_sec)
            raise exception
        return wrapped
    return wrapper


def retry_async(logger: Logger, num_tries: Union[int, float] = 3, idle_time_sec: float = .1):
    def wrapper(fn):
        @wraps(fn)
        async def wrapped(*args, **kwargs):
            num_try, exception = 0, None
            while num_try <= num_tries:
                try:
                    return await fn(*args, **kwargs)
                except Exception as ex:
                    logger.info(f"{wrapper.__str__()} - Retry: {ex}")
                    exception = ex
                    num_try += 1
                await aio_sleep(idle_time_sec)
            raise exception
        return wrapped
    return wrapper
