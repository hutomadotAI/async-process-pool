"""Tests for watchdog """
# flake8: noqa
import asyncio
import logging
import pytest

from async_process_pool.watchdog import Watchdog

pytestmark = pytest.mark.asyncio

def _get_logger():
    logger = logging.getLogger('hu.watchdog.test')
    return logger


class Callback:
    def __init__(self):
        self.counter = 0
        self.logger = _get_logger()

    def __call__(self, *args, **kwargs):
        self.logger.info("In callback")
        self.counter += 1


async def test_watchdog_init():
    callback = Callback()
    watchdog = Watchdog(0.05, callback, "test1")
    watchdog.reset_watchdog()
    watchdog.cancel()


async def test_watchdog_fires():
    callback = Callback()
    watchdog = Watchdog(0.05, callback, "test2")
    watchdog.reset_watchdog()
    await asyncio.sleep(0.15)
    assert callback.counter == 1


async def test_watchdog_resets():
    callback = Callback()
    watchdog = Watchdog(0.05, callback, "test2")
    watchdog.reset_watchdog()
    for ii in range(0, 5):
        await asyncio.sleep(0.02)
        watchdog.reset_watchdog()
    assert callback.counter == 0
