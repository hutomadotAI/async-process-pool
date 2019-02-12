import asyncio
import asyncio_utils
import asyncio_utils.process_pool as pp
import multiprocessing


class OomMessage(asyncio_utils.Message):
    pass


class OomResponse(asyncio_utils.Response):
    pass


class OomWorker(asyncio_utils.ProcessWorkerABC):
    def __init__(self, pool, asyncio_loop):
        super().__init__(pool, asyncio_loop)
        self.mem_holder = []

    async def process_message(self, msg):
        print("In process_message")
        self.mem_holder.append(bytearray(1024*1024*100))
        await self.pool.send_message_out(OomResponse(msg))


async def main_async():
    print("In main_async!")
    mp_manager = multiprocessing.Manager()
    loop = asyncio.get_event_loop()
    training_pool = pp.AsyncProcessPool(
        mp_manager, 'Training_pool', loop,
        1, 1, 1)
    await training_pool.initialize_processes(
        OomWorker)
    for ii in range(100):
        print("Sending {}".format(ii))
        msg = OomMessage()
        await training_pool.do_work(msg)
    await training_pool.shutdown()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_async())


if __name__ == "__main__":
    main()
