"""Watchdog class"""
import asyncio
import logging


def _get_logger():
    logger = logging.getLogger('hu.watchdog')
    return logger


class Watchdog:
    """Watchdog class"""

    def __init__(self,
                 timeout_seconds,
                 callback,
                 name="Unnamed"):
        self.timeout_seconds = float(timeout_seconds)
        self.callback = callback
        self.logger = _get_logger()
        self.name = name
        self.__call_handle = None
        self.logger.info("'%s' watchdog created: will time out after %.2fs",
                         self.name, self.timeout_seconds)

    def reset_watchdog(self):
        """Reset the watchdog"""
        self.cancel()
        loop = asyncio.get_running_loop()
        self.__call_handle = loop.call_later(
            self.timeout_seconds, self.__internal_callback)

    def cancel(self):
        """Cancel the watchdog"""
        if self.__call_handle is not None:
            self.logger.debug("'%s' watchdog was reset", self.name)
            self.__call_handle.cancel()

    def __internal_callback(self):
        self.logger.info(
            "'%s' watchdog timed out after %.2fs, firing callback", self.name,
            self.timeout_seconds)
        self.callback()
