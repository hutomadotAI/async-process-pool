"""Tests for watchdog """
# pylint: skip-file
import asyncio
import logging

import hu_logging
from asyncio_utils import Watchdog

import pytest


def _get_logger():
    logger = hu_logging.get_logger('hu.watchdog.test', console_log_level=logging.INFO)
    return logger


class Callback:
    def __init__(self):
        self.counter = 0
        self.logger = _get_logger()

    def __call__(self, *args, **kwargs):
        self.logger.info("In callback")
        self.counter += 1


async def test_watchdog_init(loop):
    callback = Callback()
    watchdog = Watchdog(0.05, callback, loop, "test1")
    watchdog.reset_watchdog()
    watchdog.cancel()


async def test_watchdog_fires(loop):
    callback = Callback()
    watchdog = Watchdog(0.05, callback, loop, "test2")
    watchdog.reset_watchdog()
    await asyncio.sleep(0.15, loop=loop)
    assert callback.counter == 1


async def test_watchdog_resets(loop):
    callback = Callback()
    watchdog = Watchdog(0.05, callback, loop, "test2")
    watchdog.reset_watchdog()
    for ii in range(0, 5):
        await asyncio.sleep(0.02, loop=loop)
        watchdog.reset_watchdog()
    assert callback.counter == 0
