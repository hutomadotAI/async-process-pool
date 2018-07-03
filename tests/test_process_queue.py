"""Tests for Process Queue"""
# flake8: noqa
import concurrent.futures
import multiprocessing
import queue

import asyncio_utils.async_process_queue as a_queue

import pytest

async def test_queue_one(loop):
    # The following are NOT picklable
    thread_executor = concurrent.futures.ThreadPoolExecutor()

    # These are picklable
    manager = multiprocessing.Manager()

    qqq = a_queue.create_async_process_queue(manager, loop, thread_executor, 1)

    await qqq.put_async(1)


async def test_queue_two_fails_nowait(loop):
    # The following are NOT picklable
    thread_executor = concurrent.futures.ThreadPoolExecutor()

    # These are picklable
    manager = multiprocessing.Manager()

    qqq = a_queue.create_async_process_queue(manager, loop, thread_executor, 1)

    await qqq.put_async(1)

    with pytest.raises(queue.Full):
        qqq.put_nowait(2)


async def test_queue_two_fails_timeout(loop):
    thread_executor = concurrent.futures.ThreadPoolExecutor()

    # These are picklable
    manager = multiprocessing.Manager()

    qqq = a_queue.create_async_process_queue(manager, loop, thread_executor, 1)

    await qqq.put_async(1)

    with pytest.raises(queue.Full):
        await qqq.put_async(2, timeout=0.05)


async def test_queue_timeout(loop):
    thread_executor = concurrent.futures.ThreadPoolExecutor()

    # These are picklable
    manager = multiprocessing.Manager()

    qqq = a_queue.create_async_process_queue(manager, loop, thread_executor, 1)

    with pytest.raises(queue.Empty):
        await qqq.get_async(0.05)
