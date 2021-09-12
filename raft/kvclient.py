import asyncio
import click
import logging
import pickle

import raft.messages
import raft.transport
import raft.kvserver
import raft.settings


async def send_request(cmd: str, key: str, value: str) -> None:
    if cmd == 'get':
        message = raft.messages.GetMessage(key=key)
    elif cmd == 'set':
        message = raft.messages.SetMessage(key=key, value=value)
    else:
        message = raft.messages.DeleteMessage(key=key)

    pickled = pickle.dumps(message)

    async def connect_to_server(message, index: int):

        address = raft.settings.KV_STORE[index]

        try:
            reader, writer = await asyncio.open_connection(*address)
        except ConnectionRefusedError:
            return await connect_to_server(message, index + 1)

        await raft.transport.send_message(writer, pickled)

        res = pickle.loads(await raft.transport.recv_message(reader))

        if isinstance(res, raft.kvserver.LeaderInfo):
            if res.leader_id is None:
                next_index = index + 1
            else:
                next_index = res.leader_id
            return await connect_to_server(message, next_index)
        else:
            return res

    res = await connect_to_server(pickled, 0)
    logger.info('Got response form KVServer: %s', res)


@click.command()
@click.option('-c', '--command', type=click.Choice(['get', 'set', 'delete']))
@click.option('-k', '--key', required=True)
@click.option('-v', '--value', required=False)
def main(command, key, value):
    if command == 'set' and value is None:
        raise AssertionError('Value needs to be specified for "set" command!')
    asyncio.run(send_request(command, key, value))


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    main()
