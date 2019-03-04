# async-process-pool
## Description
Process pool and queue that is compatible with Python 3.7's asyncio.

For more information about the library, see the library description at [src/README.md](src/README.md).

## Building and testing
### Dependencies
The library depends on Python 3.7 and uses [pipenv](https://pipenv.readthedocs.io) to describe its dependencies, in particular for testing.

### Testing
The Pipfile from `pipenv` declares what version of `pytest` and it's plugins you need to successful run the tests.

Once `pipenv` is installed, dependencies are obtained by using `pipenv install --dev`.

It is recommended to use `pipenv shell` to run tests.

Then, you are able to run `pytest` to run the unit and integration tests.
