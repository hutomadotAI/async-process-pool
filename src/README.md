# async-process-pool utility library

This library requires Python 3.7 or greater to be able to run. The library package installer will block attempts to use Python 3.6 or lower.

The library is tested on Python 3.7, taking advantage of significant improvements to the `asyncio` API and implementation in Python.
It is tested most heavily on Debian/Ubuntu Linux, but known to work on Microsoft Windows.

- Async Process Queue: allows awaitable cross-process queues of work to be written.
- Process Pool: allows for a pool of processes to be created which can take work. 
Communication to these processes is via the async process queue defined in this library.
- Watchdog: a asyncio implementation of a watchdog timer. It will fire an action if it is not reset within a set time. For example: re-registering a worker if a heartbeat signal is not received at least every 5 seconds.
