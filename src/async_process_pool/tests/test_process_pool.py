"""Tests for Process Pool"""
# flake8: noqa
import asyncio
import logging
import multiprocessing

import async_process_pool.process_pool as a_pool
import queue
import pytest

pytestmark = pytest.mark.asyncio

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
    def __init__(self, pool):
        super().__init__(pool)
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
        print("Started {}", msg)
        if msg.fail_me:
            raise RuntimeError("I agreed to fail this test")
        input = msg.input
        if msg.wait_for_cancel:
            while True:
                print("Not cancelled, sleeping")
                await asyncio.sleep(0.5)
                self.pool.check_for_cancel(msg)
        resp = a_pool.Response(msg, output=input * 4)
        return resp


async def test_create_ok(manager):
    a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=1)


async def test_create_fail_wrong_type(manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        a_pool.AsyncProcessPool(
            manager, "SimpleTest", num_processes='hi')


async def test_create_fail_0_size(manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        a_pool.AsyncProcessPool(
            manager, "SimpleTest", num_processes=0)


async def test_create_fail_20000_processes(manager):
    with pytest.raises(a_pool.ProcessPoolConfigurationError):
        a_pool.AsyncProcessPool(
            manager, "SimpleTest", num_processes=20000)


async def test_pool1_ping(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_ping2(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the higher level do_work function
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool4_ping3(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=4)
    await pool.initialize_processes(ProcessWorker)
    msg = Ping()
    # use the higher level do_work function
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_fail(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.ErrorResponse)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool4_fail(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=4)
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the low level messaging functions
    await pool.send_message_in(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.ErrorResponse)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_fail2(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = FailMe()
    # use the higher level messaging functions
    with pytest.raises(a_pool.FailedJobError):
        await pool.do_work(msg)
    await pool.shutdown()


async def test_pool1_job1(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(3)
    # use the higher level messaging functions
    resp = await pool.do_work(msg)
    assert isinstance(resp, a_pool.Response)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool1_jobfail1(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(3, fail_me=True)
    # use the higher level messaging functions
    with pytest.raises(a_pool.FailedJobError):
        await pool.do_work(msg)
    await pool.shutdown()


async def test_pool8_ping4(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=8)
    await pool.initialize_processes(ProcessWorker)
    msgs = [Ping() for x in range(4)]
    # use the higher level do_worklist function
    resp = await pool.do_worklist(msgs)
    logger = _get_logger()
    logger.debug('received {}'.format(resp))
    await pool.shutdown()


async def test_pool8_ping4_fail1(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=8)
    await pool.initialize_processes(ProcessWorker)
    msgs = [Ping() for x in range(4)]
    msgs.append(FailMe())
    # use the higher level do_worklist function
    with pytest.raises(a_pool.FailedJobError):
        resp = await pool.do_worklist(msgs)
        logger = _get_logger()
        logger.debug('received {}'.format(resp))

    await pool.shutdown()


async def test_pool8_fail1_ping4(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=8)
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
    await asyncio.sleep(0.05)
    await pool.do_work(Ping())
    assert pool.get_queue_status() == (0, 0, 0)
    await pool.shutdown()


async def test_cancel_1(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # use the low level messaging functions
    await pool.send_message_in(msg)
    await asyncio.sleep(0.1)
    await pool.send_cancel(msg)
    resp = await pool.get_message_out()
    assert isinstance(resp, a_pool.CancelResponse)
    await pool.shutdown()


async def test_cancel_2(manager):
    pool = a_pool.AsyncProcessPool(manager, "SimpleTest")
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # use the low level messaging functions
    await pool.send_message_in(msg)
    await asyncio.sleep(0.1)
    await pool.send_cancel(msg)
    with pytest.raises(a_pool.JobCancelledError):
        await pool.get_message_out_with_id(msg.msg_id)
    await pool.shutdown()


async def send_cancel_after_100ms(pool, msg):
    logger = _get_logger()
    logger.debug('In send_cancel_after_100ms - sleeping')
    await asyncio.sleep(0.5)
    logger.debug('In send_cancel_after_100ms - awake')
    await pool.send_cancel(msg)


async def test_cancel_3(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=1)
    await pool.initialize_processes(ProcessWorker)

    msgs = [DoWork(4, wait_for_cancel=True) for x in range(4)]
    future1 = asyncio.ensure_future(
        send_cancel_after_100ms(pool, msgs[0]))
    future2 = asyncio.ensure_future(pool.do_worklist(msgs))
    tasks = [future1, future2]
    combined_future = asyncio.gather(*tasks)
    with pytest.raises(a_pool.JobCancelledError):
        await combined_future

    await pool.shutdown()


async def test_cancel_from_q_in(manager):
    """This test can only pass if the cancelled message is handled while it is still queued.
    This is because every task will block,
    we have only one execution process
    and we cancel the SECOND message that was queued."""
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", num_processes=1)
    await pool.initialize_processes(ProcessWorker)

    msgs = [DoWork(4, wait_for_cancel=True) for x in range(4)]
    tasks = []
    cancel_future = asyncio.ensure_future(
    send_cancel_after_100ms(pool, msgs[1]))
    tasks.append(cancel_future)
    tasks.append(asyncio.ensure_future(pool.do_worklist(msgs)))
    combined_future = asyncio.gather(*tasks)
    with pytest.raises(a_pool.JobCancelledError):
        await combined_future

    await pool.shutdown()


async def test_pool_raises_if_overflow(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)
    msg = DoWork(4, wait_for_cancel=True)
    # This one will be run
    await pool.send_message_in(msg)
    # This one will be queued
    await pool.send_message_in(msg)

    # This one will overflow the queue
    with pytest.raises(queue.Full):
        await pool.send_message_in(msg, timeout=0.05)


async def test_get_message_out_timeout(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Try and read message with 0.05s timeout
    with pytest.raises(queue.Empty):
        await pool.get_message_out(timeout=0.05)


async def test_get_message_out_with_id_timeout(manager):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Create message but never send it
    msg = DoWork(4, wait_for_cancel=True)
    # Therefore there can be no reply within 0.05s
    with pytest.raises(queue.Empty):
        await pool.get_message_out_with_id(msg, timeout=0.05)


async def test_pool_unhealthy_on_dead_worker_1(manager, mocker):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Patch is_alive to indicate our worker process died
    mock_call = mocker.patch('multiprocessing.Process.is_alive')
    mock_call.return_value = False

    msg = Ping()
    with pytest.raises(a_pool.PoolUnhealthyError):
        await pool.send_message_in(msg)

async def test_pool_unhealthy_on_dead_worker_2(manager, mocker):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    # Patch is_alive to indicate our worker process died
    mock_call = mocker.patch('multiprocessing.Process.is_alive')
    mock_call.return_value = False

    with pytest.raises(a_pool.PoolUnhealthyError):
        await pool.get_message_out()

async def test_pool_unhealthy_on_dead_worker_3(manager, mocker):
    pool = a_pool.AsyncProcessPool(
        manager, "SimpleTest", q_in_size=1, q_out_size=2)
    await pool.initialize_processes(ProcessWorker)

    with pytest.raises(a_pool.PoolUnhealthyError):
        msg = DoWork("Hi", wait_for_cancel=True)
        work_task = asyncio.ensure_future(pool.do_work(msg))
        await asyncio.sleep(1)

        # check that the work_task is still "running"
        assert not work_task.done()

        # NOW - once the message was sent and we waited a bit, now
        # patch is_alive to indicate our worker process died
        mock_call = mocker.patch('multiprocessing.Process.is_alive')
        mock_call.return_value = False

        # This should now cause the do_work operation to bomb
        await work_task
    # Shutdown pool to cleanup the hanging background process
    await pool.shutdown()

if __name__ == "__main__":
    pytest.main(args=['test_process_pool.py::test_pool8_ping4', '-s'])
    # pytest src/tests/test_wnet.py::test_multiprocess_3
