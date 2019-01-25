import aiohttp
import traceback
import logging


def _get_logger():
    logger = logging.getLogger('hu.aiohttp_wrapper_caller')
    return logger


class ExceptionWrappedCaller:
    """Put an exception logging wrapper around all the endpoints.
       This is preferable to using aiohttp middleware as we control
       that here without upstream involvement"""

    def __init__(self, call_to_wrap):
        self.call_to_wrap = call_to_wrap
        self.logger = _get_logger()

    async def __call__(self, *args, **kwargs):
        try:
            response = await self.call_to_wrap(*args, **kwargs)
        except aiohttp.web_exceptions.HTTPException:
            # assume if we're throwing this that it's already logged
            raise
        except Exception:
            self.logger.exception("Unexpected exception in call")

            error_string = "Internal Server Error\n" + traceback.format_exc()
            raise aiohttp.web_exceptions.HTTPInternalServerError(
                text=error_string)
        return response
