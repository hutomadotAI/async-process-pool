# AsyncIO utils

This contains common code for async IO which is used as part of AIs.

Async IO requires Python 3.5 or higher to run.

- Async Process Queue: allows awaitable cross-process queues
- Process Pool: allows for a pool of processes to be created which can take work. 
Communication to these processes is via an async process queue.