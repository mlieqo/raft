import asyncio
import click
import logging

import raft.controller
import raft.kvserver
import raft.settings
from functools import wraps


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.command()
@click.option('-n', '--node', default=0, help='KVStore/Node index', type=int)

@coro
async def main(node):

    assert node in raft.settings.KV_STORE

    logging.basicConfig(
        level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    kvserver = raft.kvserver.KVServer(node)

    try:
        await kvserver.run()
    except Exception as e:
        logger.error('Unhandled exception while running KVServer - %s', e)
    finally:
        await kvserver.stop()


if __name__ == '__main__':
    main()
