"""
Asynchronous process queue
Based on
http://stackoverflow.com/q/24687061/694641
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor


def create_async_process_queue(manager, executor, maxsize):
    """Create an async process queue on a given manager"""
    queue = manager.Queue(maxsize=maxsize)
    proc_queue = _ProcQueue(queue)
    proc_queue.set_process_variables(executor)
    return proc_queue


def async_process_queue_worker(func, q_in, q_out, *args, **kwargs):
    """This is the worker function for a sub-process"""
    # make sure that we create a new event loop for the new process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # set up the queues to operate correctly on this process
    executor = ThreadPoolExecutor()
    q_in.set_process_variables(executor)
    q_out.set_process_variables(executor)
    loop.run_until_complete(func(q_in, q_out, *args, **kwargs))


class _ProcQueue:
    def __init__(self, queue):
        self._queue = queue
        self._executor = None
        self._cancelled_join = False

    def set_process_variables(self, executor):
        """Set per process variables needed for operation of the queue"""
        self._executor = executor

    def __getstate__(self):
        """This function is used to control pickling of this object so it can
        be shared across processes"""
        state = self.__dict__.copy()
        del state['_executor']
        return state

    def __setstate__(self, state):
        """This function is used to control pickling of this object so it can
        be shared across processes"""
        # We need this function to be present but it just needs to do the standard job
        # of updating the internal dictionary based on state
        self.__dict__.update(state)

    def __getattr__(self, name):
        if name in ['qsize', 'empty', 'full', 'put', 'put_nowait', 'get', 'get_nowait', 'close']:
            return getattr(self._queue, name)
        else:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))

    async def put_async(self, item, timeout=None):
        """Async put method"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.put, item, True, timeout)

    async def get_async(self, timeout=None):
        """Async get method"""
        loop = asyncio.get_running_loop()
        val = await loop.run_in_executor(self._executor, self.get, True, timeout)
        return val

    def cancel_join_thread(self):
        """Queue implementation"""
        self._cancelled_join = True
        self._queue.cancel_join_thread()

    def join_thread(self):
        """Queue implementation"""
        self._queue.join_thread()
        if self._executor and not self._cancelled_join:
            self._executor.shutdown()
