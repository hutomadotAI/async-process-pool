import asyncio
import logging
import asyncio_utils
import asyncio_utils.process_pool as pp
import multiprocessing
from pathlib import Path


class OomMessage(asyncio_utils.Message):
    pass


class OomResponse(asyncio_utils.Response):
    pass


class OomWorker(asyncio_utils.ProcessWorkerABC):
    def __init__(self, pool, asyncio_loop):
        super().__init__(pool, asyncio_loop)
        self.mem_holder = []
        self.file = Path("/tmp/datafile1")

    @asyncio_utils.job_runner
    async def process_message(self, msg):
        logging.info("WORKER: process_message")
        self.mem_holder.append(bytearray(1024*1024*10))
        await asyncio.sleep(2)
        logging.info("WORKER: Leaving process_message")
        return OomResponse(msg)


async def main_async():
    print("In main_async!")
    logging.basicConfig(level=logging.DEBUG)
    mp_manager = multiprocessing.Manager()
    loop = asyncio.get_event_loop()
    training_pool = pp.AsyncProcessPool(
        mp_manager, 'Training_pool', loop,
        1, 1, 1)
    await training_pool.initialize_processes(
        OomWorker)
    try:
        for ii in range(100000):
            logging.info("REQUESTER: Sending #{}".format(ii))
            msg = OomMessage()
            await training_pool.do_work(msg)
            logging.info("REQUESTER: Completed #{}".format(ii))
    finally:
        await training_pool.shutdown()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_async())


if __name__ == "__main__":
    main()
