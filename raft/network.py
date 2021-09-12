from typing import Tuple, Callable, Dict

import asyncio
import contextlib
import logging
import pickle

import raft.messages


class RaftProtocol:
    """
    Communication protocol for communicating with the rest of the raft nodes.

    Connections are accepted on TCP server, but messages are sent from different sockets,
    which means the connections are duplicated. Cached connections to TCP servers are
    stored in `self._connections`.
    """

    MESSAGE_HEADER_SIZE = 10

    def __init__(
        self,
        node_id: int,
        raft_nodes: Dict[str, Tuple[str, int]],
        on_message_callback: Callable[[raft.messages.NodeMessage], None],
    ):
        self._node_id: int = node_id
        # cached connections -> key is server index and value is reader/writer
        self._connections: Dict[
            int, Tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = {}
        self._node_addresses = raft_nodes
        self._on_message_callback = on_message_callback
        self._server = None
        self._server_task = None
        self._logger = logging.getLogger(
            f'{self.__class__.__name__}<node_id:{self._node_id}>'
        )

    async def start(self) -> None:
        self._server_task = asyncio.create_task(self._start_server())

    async def stop(self) -> None:
        self._server_task.cancel()
        self._server.close()
        with contextlib.suppress(asyncio.CancelledError):
            await self._server_task

    async def send_message(
        self, message: raft.messages.NodeMessage, server_id: int
    ) -> None:
        """
        Send message to TCP server of another node. Connections are stored in `self._connections`
        and reused until the writer is closed.
        """

        if (
            server_id not in self._connections
            or self._connections[server_id][1].is_closing()
        ):
            self._logger.debug('Opening new connection with server %s', server_id)
            try:
                self._connections[server_id] = await asyncio.open_connection(
                    *self._node_addresses[server_id]
                )
            except Exception as e:
                self._logger.debug(
                    'Error - %s connecting to %s', e, self._node_addresses[server_id]
                )
                return

        self._logger.debug(f'sending message %s to %s', message, server_id)
        pickled = pickle.dumps(message)
        await self._send(self._connections[server_id][1], pickled)

    async def _send(self, writer: asyncio.StreamWriter, message: bytes) -> None:
        """
        Sends `message` with `writer`, which means connection has to be open before attempting to
        send anything. It also prepends header of size `HEADER_SIZE` at the beginning of message,
        with information about the message length
        """

        header = b'%10d' % len(message)
        writer.write(header + message)
        try:
            await writer.drain()
        except Exception as e:
            self._logger.error('Error - %s, while sending message - %s', e, message)

    async def _receive_message(self, reader: asyncio.StreamReader) -> bytes:
        """
        Reads message from `reader`. First it reads `MESSAGE_HEADER_SIZE`, with information about
        the message length, and then reads exactly the message length.
        """

        try:
            msg_length = int(await reader.readexactly(self.MESSAGE_HEADER_SIZE))
            full_message = await reader.readexactly(msg_length)
            return full_message
        except asyncio.IncompleteReadError as e:
            self._logger.debug('Error while reading - %s', e)

    async def _start_server(self) -> None:
        """
        Start TCP server at the host / port addresses defined in `self._node_addresses`
        """

        self._server = await asyncio.start_server(
            self._handle_client_request, *self._node_addresses[self._node_id]
        )

        self._logger.info(f'Started listening on {self._node_addresses[self._node_id]}')
        async with self._server:
            await self._server.serve_forever()

    async def _handle_client_request(
        self, reader: asyncio.StreamReader, _: asyncio.StreamWriter
    ) -> None:
        """
        Infinitely reads messages from reader, and calls `self._on_message_callback` on the message.
        """

        while True:
            message = await self._receive_message(reader)
            if not message:
                return
            unpickled = pickle.loads(message)
            self._logger.info('Received message %s', unpickled)
            self._on_message_callback(unpickled)
