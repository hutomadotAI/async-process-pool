# Async Process Pool and Queue

This contains an `asyncio` aware multiprocessing utility that allows work to be scheduled across multiple processes.

It consists of the following sub-utilities:
- Async Process Queue: allows awaitable cross-process queues
- Process Pool: allows for a pool of processes to be created which can take work. 
Communication to these processes is via an async process queue.
- Watchdog: Calls back after a set time, unless it is reset.
