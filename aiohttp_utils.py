import aiohttp
from aiohttp import web
import traceback
import logging


def _get_logger():
    logger = logging.getLogger('hu.aiohttp_utils')
    return logger


@web.middleware
async def aiohttp_log_error_middleware(request, handler):
    try:
        response = await handler(request)
    except aiohttp.web_exceptions.HTTPException:
        # assume if we're throwing this that it's already logged
        raise
    except Exception as exc:
        _get_logger().exception("Unexpected exception in call")

        error_string = "Internal Server Error\n" + traceback.format_exc()
        raise aiohttp.web_exceptions.HTTPInternalServerError(
            text=error_string)
    return response
