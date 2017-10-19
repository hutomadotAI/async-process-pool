import asyncio
import logging

def _get_logger():
    logger = logging.getLogger('hu.watchdog')
    return logger


class Watchdog:
    def __init__(self, timeout_seconds, callback, asyncio_loop, name="Unnamed"):
        self.timeout_seconds = float(timeout_seconds)
        self.callback = callback
        self.asyncio_loop = asyncio_loop
        self.logger = _get_logger()
        self.name = name
        self.__call_handle = None

    def reset_watchdog(self):
        self.cancel()
        self.__call_handle = self.asyncio_loop.call_later(self.timeout_seconds, self.__internal_callback)

    def cancel(self):
        if self.__call_handle is not None:
            self.logger.debug("'{}' watchdog was reset".format(self.name))
            self.__call_handle.cancel()

    def __internal_callback(self):
        self.logger.info(
            "'{}' watchdog timed out after {}s, firing callback".format(self.name,
                                                                        self.timeout_seconds))
        self.callback()
