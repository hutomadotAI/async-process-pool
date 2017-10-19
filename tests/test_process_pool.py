"""Tests for Process Pool"""
# pylint: skip-file
import asyncio
import logging
import multiprocessing

import asyncio_utils.process_pool as a_pool
import queue
import pytest

@pytest.fixture
def manager():
    manager = multiprocessing.Manager()
    return manager

def _get_logger():
    logger = logging.getLogger('hu.process_pool.test')
    return logger


class Ping(a_pool.Message):
    """Minimal derived message"""

    def __init__(self):
        # MUST call super class' init
        super().__init__()


class FailMe(a_pool.Message):
    """Minimal derived message"""

    def __init__(self):
        # MUST call super class' init
        super().__init__()


class DoWork(a_pool.Message):
    """Minimal derived message"""

    def __init__(self, input, fail_me=False, wait_for_cancel=False):
        # MUST call super class' init
        super().__init__()
        self.input = input
        self.fail_me = fail_me
        self.wait_for_cancel = wait_for_cancel


class ProcessWorker(a_pool.ProcessWorkerABC):
    def __init__(self, pool, asyncio_loop):
        super().__init__(pool, asyncio_loop)
        self.logger = _get_logger()

    async def __aenter__(self):
        self.logger.info('In ProcessWorker.__aenter__()')
        return self

    async def __aexit__(self, exc_type, exc, traceb):
        self.logger.info('In ProcessWorker.__aexit__()')

    async def process_message(self, msg):
        """Simple process pool loop"""
        if isinstance(msg, Ping):
            resp = a_pool.Response(msg)
            await self.pool.send_message_out(resp)
        elif isinstance(msg, FailMe):
            resp = a_pool.ErrorResponse(msg, "You've been failed")
            await self.pool.send_message_out(resp)
        elif isinstance(msg, DoWork):
            await self.do_work_fn(msg)

    @a_pool.job_runner
    async def do_work_fn(self, msg):
        if msg.fail_me:
            raise RuntimeError("I agreed to fail this test")
        input = msg.input
        if msg.wait_for_cancel:
            while True:
                await asyncio.sleep(0.1, self.asyncio_loop)
                self.pool.check_for_cancel(msg)
        resp = a_pool.Response(msg, output=input * 4)
        return resp

async def test_create_ok(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=1)

async def test_create_fail_wrong_type(loop, manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes='hi')

async def test_create_fail_0_size(loop, manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=0)

async def test_create_fail_20000_processes(loop, manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=20000)

async def test_pool1_ping(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_ping2(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the higher level do_work function
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool4_ping3(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=4)
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the higher level do_work function
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_fail(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.ErrorResponse)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool4_fail(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=4)
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.ErrorResponse)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_fail2(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the higher level messaging functions
    with pytest.raises(a_pool.FailedJobError):
        await pool.do_work(msg)
    await pool.shutdown()


async def test_pool1_job1(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(3)
    # use the higher level messaging functions
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_jobfail1(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(3, fail_me=True)
    # use the higher level messaging functions
    with pytest.raises(a_pool.FailedJobError):
        await pool.do_work(msg)
    await pool.shutdown()


async def test_pool8_ping4(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=8)
    await pool.initialize_processes(ProcessWorker)
    msgs = [Ping() for x in range(4)]
    # use the higher level do_worklist function
    resp = await pool.do_worklist(msgs)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool8_ping4_fail1(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=8)
    await pool.initialize_processes(ProcessWorker)
    msgs = [Ping() for x in range(4)]
    msgs.append(FailMe())
    # use the higher level do_worklist function
    with pytest.raises(a_pool.FailedJobError):
        resp = await pool.do_worklist(msgs)
        logger = _get_logger()
        logger.debug('received {}'.format(resp))

    await pool.shutdown()


async def test_pool8_fail1_ping4(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=8)
    await pool.initialize_processes(ProcessWorker)
    msgs = [FailMe()]
    msgs += [Ping() for x in range(4)]

    # use the higher level do_worklist function
    with pytest.raises(a_pool.FailedJobError):
        resp = await pool.do_worklist(msgs)
        logger = _get_logger()
        logger.debug('received {}'.format(resp))

    # check that the messages that got posted after the failure are not
    # in the queue as they were ignored later as part of the single Ping()
    await asyncio.sleep(0.05, loop=loop)
    await pool.do_work(Ping())
    assert pool.get_queue_status() == (0, 0, 0)
    await pool.shutdown()


async def test_cancel_1(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # use the low level messaging functions
    await pool.send_message_in(msg)
    await asyncio.sleep(0.1, loop=loop)
    await pool.send_cancel(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.CancelResponse)
    await pool.shutdown()


async def test_cancel_2(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # use the low level messaging functions
    await pool.send_message_in(msg)
    await asyncio.sleep(0.1, loop=loop)
    await pool.send_cancel(msg)
    with pytest.raises(a_pool.JobCancelledError):
        await pool.get_message_out_with_id(msg.msg_id)
    await pool.shutdown()


async def send_cancel_after_100ms(pool, loop, msg):
    logger = _get_logger()
    logger.debug('In send_cancel_after_100ms - sleeping')
    await asyncio.sleep(0.1, loop=loop)
    logger.debug('In send_cancel_after_100ms - awake')
    await pool.send_cancel(msg)


async def test_cancel_3(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=1)
    await pool.initialize_processes(ProcessWorker)

    msgs = [DoWork(4, wait_for_cancel=True) for x in range(4)]
    future1 = asyncio.ensure_future(send_cancel_after_100ms(pool, loop, msgs[0]), loop=loop)
    future2 = asyncio.ensure_future(pool.do_worklist(msgs), loop=loop)
    tasks = [future1, future2]
    combined_future = asyncio.gather(*tasks)
    with pytest.raises(a_pool.JobCancelledError):
        await combined_future

    await pool.shutdown()

async def test_cancel_from_q_in(loop, manager):
    """This test can only pass if the cancelled message is handled while it is still queued. This is because every task
    will block, we have only one execution process and we cancel the SECOND message that was queued."""
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, num_processes=1)
    await pool.initialize_processes(ProcessWorker)

    msgs = [DoWork(4, wait_for_cancel=True) for x in range(4)]
    tasks = []
    cancel_future = asyncio.ensure_future(send_cancel_after_100ms(pool, loop, msgs[1]), loop=loop)
    tasks.append(cancel_future)
    tasks.append(asyncio.ensure_future(pool.do_worklist(msgs), loop=loop))
    combined_future = asyncio.gather(*tasks)
    with pytest.raises(a_pool.JobCancelledError):
        await combined_future

    await pool.shutdown()

async def test_pool_raises_if_overflow(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # This one will be run
    await pool.send_message_in(msg)
    # This one will be queued
    await pool.send_message_in(msg)

    # This one will overflow the queue
    with pytest.raises(queue.Full):
        await pool.send_message_in(msg, timeout=0.05)

async def test_get_message_out_timeout(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Try and read message with 0.05s timeout
    with pytest.raises(queue.Empty):
        await pool.get_message_out(timeout=0.05)

async def test_get_message_out_with_id_timeout(loop, manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest", loop, q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Create message but never send it
    msg = DoWork(4, wait_for_cancel=True)
    # Therefore there can be no reply within 0.05s
    with pytest.raises(queue.Empty):
        await pool.get_message_out_with_id(msg, timeout=0.05)

if __name__ == "__main__":
    pytest.main(args=['test_process_pool.py::test_pool8_ping4', '-s'])
    # pytest src/tests/test_wnet.py::test_multiprocess_3
