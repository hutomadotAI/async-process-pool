"""
Process pool with async input/output queues
"""
import abc
import asyncio
import concurrent.futures
import logging
import multiprocessing
import queue
import random
import traceback
import uuid

import async_process_pool.async_process_queue as proc_q


def _get_logger():
    logger = logging.getLogger('hu.process_pool')
    return logger


# Exceptions
class Error(Exception):
    """Base exception for this module"""
    pass


class ProcessPoolConfigurationError(Error):
    """Exception used to indicate Process Pool is invalidly configured"""
    pass


class ProcessShutdownException(Error):
    """Exception used to end process"""
    pass


class PoolUnhealthyError(Error):
    """Exception used when pool is unhealthy due to crashed sub-process"""
    pass


# Messages
class Message(object):
    """
    Message object - takes all keyword arguments and adds them to the
    members of Message.
    That means that kwargs has to be well behaved:
    - no keywords (e.g. def, class)
    - no invalid names (e.g. 2, ?, :/)
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.__msg_id = str(uuid.uuid4())

    @property
    def msg_id(self):
        """The message ID"""
        return self.__msg_id

    def __repr__(self):
        # There really shouldn't be recursion here, so this should be OK
        sss = "{}({})".format(self.__class__.__name__, self.__dict__)
        return sss


class ProcessShutdownMessage(Message):
    """Define a common message for process shutdown"""
    pass


class Response(Message):
    """Response object, derives from Message"""

    def __init__(self, msg_in_response_to: Message, **kwargs):
        super().__init__(**kwargs)
        self.re_msg_id = msg_in_response_to.msg_id


class ErrorResponse(Response):
    """Error Response object, derives from Response"""

    def __init__(self, msg_in_response_to: Message, error_description,
                 **kwargs):
        super().__init__(msg_in_response_to, error=error_description, **kwargs)


class CancelResponse(Response):
    """Job cancelled response object, derives from Response"""

    def __init__(self, msg_in_response_to: Message, **kwargs):
        super().__init__(msg_in_response_to, **kwargs)


# Exception carrying message
class FailedJobError(Error):
    """Exception to signal that a job failed"""

    def __init__(self, message: str):
        super().__init__()
        self.message = message

    def __str__(self):
        sss = "{}:{}".format(self.__class__.__name__, self.message)
        return sss


class JobCancelledError(Error):
    """Exception to signal that a job as cancelled"""

    def __init__(self):
        super().__init__()


class ProcessWorkerABC(abc.ABC):
    """Process worker Abstract Base Class"""

    def __init__(self, pool):
        self.pool = pool

    # Support for async with - override these for initialization/shutdown
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceb):
        pass

    async def async_process_loop(self):
        """Wrapping function"""
        try:
            while True:
                msg = await self.pool.get_message_in()
                try:
                    await self.process_message(msg)
                except ProcessShutdownException:
                    raise
                except JobCancelledError:
                    # if a cancelled job, raise it
                    cancel_resp = CancelResponse(msg)
                    await self.pool.send_message_out(cancel_resp)
                except FailedJobError as fail:
                    # if a chained failed job, pass the original
                    error_resp = ErrorResponse(msg, str(fail))
                    await self.pool.send_message_out(error_resp)
                except Exception:
                    # Catch exception as we don't want the whole process to die
                    description = traceback.format_exc()
                    error_resp = ErrorResponse(msg, description)
                    await self.pool.send_message_out(error_resp)
        except ProcessShutdownException:
            # we're being shut down!
            pass

    @abc.abstractmethod
    async def process_message(self, msg):
        """Process message method"""

    def set_data(self, data):
        """Set data"""
        pass


async def worker_internal_async(process_worker_type, pool, **kwargs):
    """This is the async loop for the sub-process"""
    async with process_worker_type(pool) as process_worker:
        try:
            process_worker.set_data(kwargs)
        except Exception:
            logger = _get_logger()
            logger.exception("Failed to set data", exc_info=True)
        await process_worker.async_process_loop()


def process_pool_worker_internal(pool, process_worker_type, **kwargs):
    """This is the entry point for a sub-process"""
    # make sure that we create a new event loop for the new process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # set up the queues to operate correctly on this process
    executor = concurrent.futures.ThreadPoolExecutor()
    pool.set_process_variables(executor)
    # start the process loop
    loop.run_until_complete(
        worker_internal_async(process_worker_type, pool, **kwargs))


class AsyncProcessPool:
    """
    Async Process Pool.
    Provides a way of queuing work to a pool of processes that
    run the same task.
    By default the pool is 1 process.
    """

    def __init__(self,
                 multiprocessing_manager,
                 pool_name,
                 num_processes: int = 1,
                 q_in_size=10,
                 q_out_size=10):
        if not isinstance(num_processes, int):
            raise ProcessPoolConfigurationError(
                "num_processes must be an integer")
        if num_processes <= 0:
            raise ProcessPoolConfigurationError(
                "num_processes must be greater than 0")
        if num_processes >= 10000:
            raise ProcessPoolConfigurationError(
                "num_processes must be less than than 10000")

        # The following are NOT picklable
        self.__thread_executor = concurrent.futures.ThreadPoolExecutor()
        self.__processes = []

        # These are picklable
        self.pool_name = pool_name
        self.__manager = multiprocessing_manager
        self.__num_processes = num_processes
        self.__q_in = proc_q.create_async_process_queue(
            self.__manager, self.__thread_executor,
            q_in_size)
        self.__q_out = proc_q.create_async_process_queue(
            self.__manager, self.__thread_executor,
            q_out_size)
        self.__q_cancel = proc_q.create_async_process_queue(
            self.__manager, self.__thread_executor,
            q_in_size)
        self.__msgids_out_to_ignore = set()
        self.logger = _get_logger()

    def __getstate__(self):
        """This function is used to control pickling of this object so it can
        be shared across processes"""
        state = {'normals': {}, 'queues': {}}
        self_dict = self.__dict__.copy()
        for name, item in self_dict.items():
            if ('__thread_executor' in name
                    or '__processes' in name or '__manager' in name
                    or 'logger' in name):
                # we don't want unpickable objects
                continue
            elif isinstance(item, proc_q._ProcQueue):
                # we handle queues in a special way - get their state
                state['queues'][name] = item.__getstate__()
            else:
                # a normal item is just copied
                state['normals'][name] = item

        return state

    def __setstate__(self, state):
        """This function is used to control the unpickling of this object"""
        # restore the 'normal' items
        normals = state['normals']
        self.__dict__.update(normals)

        # restore the queues
        queues = state['queues']
        for name, queue_state in queues.items():
            # create the queue object, set its state and update this object
            qqq = proc_q._ProcQueue(None)
            qqq.__setstate__(queue_state)
            self.__dict__[name] = qqq

        # recreate the logger, as this can't be pickled
        self.logger = _get_logger()

    async def initialize_processes(self, process_worker_type, **kwargs):
        """Initialize the process pool"""
        for index in range(self.__num_processes):
            proc_name = "{}#{:02d}".format(self.pool_name, index)
            proc = multiprocessing.Process(
                name=proc_name,
                target=process_pool_worker_internal,
                args=(self, process_worker_type),
                kwargs=kwargs)
            proc.start()
            self.__processes.append(proc)

    def set_process_variables(self, executor):
        """Set the variables that must be changed in each process"""
        self.__thread_executor = executor
        self.__q_in.set_process_variables(executor)
        self.__q_out.set_process_variables(executor)
        self.__q_cancel.set_process_variables(executor)

    def get_queue_status(self):
        """Get queue status (useful for testing)"""
        in_size = self.__q_in.qsize()
        out_size = self.__q_out.qsize()
        cancel_size = self.__q_cancel.qsize()
        return (in_size, out_size, cancel_size)

    async def do_work(self, msg: Message) -> Response:
        """Send a message and await a response"""
        await self.send_message_in(msg)
        item = await self.get_message_out_with_id(msg.msg_id)
        return item

    async def do_worklist(self, msgs: []):
        """Send a set of message and await all the responses"""
        msg_ids = {msg.msg_id for msg in msgs}
        responses = []
        for msg in msgs:
            await self.send_message_in(msg)

        while len(msg_ids) > 0:
            self.logger.debug('DoWorklist remaining: {}'.format(len(msg_ids)))
            item = await self.get_message_out()
            if not hasattr(item, 're_msg_id') or item.re_msg_id not in msg_ids:
                # Not the message we are waiting for, repost it
                await self.send_message_out(item)
                await self._back_off_random_time()
                continue
            re_msg_id = item.re_msg_id

            # remove the message ID we just received from wait list
            msg_ids.remove(re_msg_id)

            # if any job is a failure, raise it here
            if isinstance(item, ErrorResponse):
                # ignore the other messages
                self.logger.warning(
                    'Failed message {} with {} outstanding'.format(
                        re_msg_id, len(msg_ids)))
                self.__msgids_out_to_ignore.update(msg_ids)
                raise FailedJobError(item.error)
            if isinstance(item, CancelResponse):
                # ignore the other messages
                self.logger.warning(
                    'Cancelled message {} with {} outstanding'.format(
                        re_msg_id, len(msg_ids)))
                self.__msgids_out_to_ignore.update(msg_ids)
                raise JobCancelledError()

            # if got here we have non-error response, add it to list
            responses.append(item)
        # all done
        return responses

    async def get_message_in(self) -> Message:
        """Async get method for input queue
        Will raise ProcessShutdownException if ProcessShutdownMessage is received"""
        val = await self.__q_in.get_async()
        if isinstance(val, ProcessShutdownMessage):
            # put the message back again
            await self.__q_in.put_async(val)
            raise ProcessShutdownException
        return val

    async def get_message_out(self, timeout=None) -> Message:
        """Async get method for output queue"""
        get_task = asyncio.ensure_future(self.__q_out.get_async(timeout))
        done = {}

        # Check pool health every second
        while get_task not in done:
            if not self._check_pool_healthy():
                get_task.cancel()
                self.logger.warning("Raising PoolUnhealthyError!")
                raise PoolUnhealthyError
            done, pending = await asyncio.wait({get_task}, timeout=1.0)

        item = get_task.result()
        if isinstance(item, ProcessShutdownMessage):
            # put the message back again
            await self.__q_out.put_async(item)
            raise ProcessShutdownException
        if hasattr(
                item,
                're_msg_id') and item.re_msg_id in self.__msgids_out_to_ignore:
            self.logger.info('Ignoring message {}'.format(item.re_msg_id))
            self.__msgids_out_to_ignore.remove(item.re_msg_id)
            # Call again
            item = await self.get_message_out(timeout)
        return item

    async def get_message_out_with_id(self, msg_id_match,
                                      timeout=None) -> Message:
        """Get message from output queue with matching ID"""
        while True:
            self.logger.debug(
                'Wait for message: msg_id {}'.format(msg_id_match))
            item = await self.get_message_out(timeout)
            if not hasattr(item,
                           're_msg_id') or item.re_msg_id != msg_id_match:
                self.logger.debug('Not found {}'.format(msg_id_match))
                # Not the message we are waiting for, repost it
                await self.send_message_out(item)
                await self._back_off_random_time()
                continue

            # if a failure, raise it here
            if isinstance(item, ErrorResponse):
                raise FailedJobError(item.error)

            # if cancelled, raise it here
            elif isinstance(item, CancelResponse):
                raise JobCancelledError()

            # if got here we have non-error response, return it
            return item

    async def send_message_in(self, msg: Message, timeout=1.0):
        """Async send method for input queue"""
        if not self._check_pool_healthy():
            self.logger.warning("Raising PoolUnhealthyError!")
            raise PoolUnhealthyError
        await self.__q_in.put_async(msg, timeout)

    async def send_message_out(self, msg: Message):
        """Async send method for output queue"""
        await self.__q_out.put_async(msg)

    def send_message_out_sync(self, msg: Message):
        """Synchronous send method for output queue. Use when in synchronous code
        and not able to await"""
        self.__q_out.put(msg)

    async def send_cancel(self, msg_to_cancel: Message):
        """Async send of cancel message"""
        id_to_cancel = msg_to_cancel.msg_id
        self.logger.info("send_cancel for {}".format(id_to_cancel))

        # attempt to find message in q_in
        found_msg = False
        msgs = []
        while True:
            try:
                msg = self.__q_in.get_nowait()
            except queue.Empty:
                # keep going until queue is drained
                break
            if msg.msg_id == id_to_cancel:
                # found the message we are looking for
                self.logger.warning(
                    "send_cancel: found {} in q_in".format(id_to_cancel))
                found_msg = True
            else:
                msgs.append(msg)

        for msg in msgs:
            # fill the queue in original order
            await self.__q_in.put_async(msg)

        if found_msg:
            # was removed from q_in, so post the cancel response
            cancel_response = CancelResponse(msg_to_cancel)
            await self.__q_out.put_async(cancel_response)
        else:
            # not found in q_in, so post the cancel message
            self.logger.warning(
                "send_cancel: add {} to q_cancel".format(id_to_cancel))
            await self.__q_cancel.put_async(id_to_cancel)

    def check_for_cancel(self, msg_to_cancel: Message):
        """Synchronous check of cancel message"""
        id_to_cancel = msg_to_cancel.msg_id

        # attempt to find message in q_cancel
        found_msg = False
        msgs = []
        while True:
            try:
                val = self.__q_cancel.get_nowait()
            except queue.Empty:
                # keep going until queue is drained
                break
            if val == id_to_cancel:
                # found the message we are looking for
                found_msg = True
            else:
                msgs.append(val)

        for msg in msgs:
            # fill the queue in original order, this will block until completed
            self.__q_cancel.put(msg)

        if found_msg:
            # we were told to cancel, raise the JobCancelledError
            raise JobCancelledError

    async def shutdown(self):
        """Async shutdown method"""
        self.logger.info('Shutting down worker processes')
        # post the viral "poison-pill" message
        await self.__q_in.put_async(ProcessShutdownMessage())
        await self.__q_out.put_async(ProcessShutdownMessage())
        for proc in self.__processes:
            # for every process that is alive, give it 1s to comply
            # we can't just terminate blindly as pytest-cov fails to spot usage
            if proc.is_alive():
                proc.join(1)
            if proc.is_alive():
                proc.terminate()
                self.logger.debug(proc.name + " was terminated")
            else:
                self.logger.debug(proc.name + " closed")

    async def _back_off_random_time(self):
        # Back off by random time in 0.01 to 0.1 second range to allow someone else to receive msg
        sleep_time = random.uniform(0.01, 0.1)
        self.logger.debug('message miss, sleep for {}s'.format(sleep_time))
        await asyncio.sleep(sleep_time)

    def _check_pool_healthy(self):
        alive_list = [True for proc in self.__processes if proc.is_alive()]
        alive = len(alive_list)
        self.logger.info("Pool processes: %d of %d alive", alive, len(self.__processes))
        return alive == len(self.__processes)


def job_runner(func):
    """
    Job runner function wrapper.
    The decorated function should return a asyncio_utils.Response.
    If the decorated function raises an exception it will send back
    a ErrorResponse.
    Allows ProcessShutdownException to pass through."""

    async def job_func(worker: ProcessWorkerABC, command, *args, **kwargs):
        """Wrapping function"""
        response = await func(worker, command, *args, **kwargs)
        await worker.pool.send_message_out(response)

    return job_func
