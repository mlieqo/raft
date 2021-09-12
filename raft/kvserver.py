from typing import Any, Union, Dict, Optional

import asyncio
import contextlib
import dataclasses
import enum
import logging
import pickle

import raft.transport as transport
import raft.messages
import raft.settings
import raft.controller


class ResponseEnum(enum.Enum):
    OK = 1
    NOT_OK = 2


@dataclasses.dataclass
class Response:
    """
    Response issued by KVStore for client
    """

    message: str


@dataclasses.dataclass
class LeaderInfo:
    """
    Sent with the information about the current leader
    """

    leader_id: Optional[int]


class KVServer:

    """
    Key -> value state machine with an underlying raft network.

    Accepts requests from client, if the raft node is leader, request is passed to the node so that
    it can be replicated to logs of other nodes. After a consensus is reached (majority of the nodes
    have replicated the record), raft node informs KVServer back that it can now apply the request
    to it's state and respond to client.
    """

    def __init__(self, index: int):
        # key -> value store
        self._store: Dict[str, Any] = {}
        self._host = raft.settings.KV_STORE[index][0]
        self._port = raft.settings.KV_STORE[index][1]
        self._server = None
        self._server_task = None
        self._raft_controller = raft.controller.RaftController(index, self.apply)
        self._logger = logging.getLogger(self.__class__.__name__)

    async def _start(self) -> None:
        await self._raft_controller.start()

    async def run(self) -> None:
        await self._start()
        await self._run_server()

    async def stop(self) -> None:
        self._server.close()
        await self._raft_controller.stop()

    async def _run_server(self) -> None:
        """
        Start TCP server for communicating with clients.
        """

        self._server = await asyncio.start_server(
            self._handle_request, self._host, self._port
        )
        self._logger.info('KV Server listening on port %s', self._port)
        async with self._server:
            await self._server.serve_forever()

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Method used for handling client requests. Message is passed to raft node to be replicated
        in logs of majority of nodes. If majority replication succeeds, request is applied to
        `self._store` and response is sent back to client.
        """

        message = await transport.recv_message(reader)
        unpickled = pickle.loads(message)
        address = writer.get_extra_info('peername')

        self._logger.info(f'Got request from {address}, request - {unpickled}')

        # TODO: leader should respond to get messages only after it received append entries response from majority
        # TODO: of the nodes. Otherwise (in case of network partition), it could be stale leader and we would be
        # TODO: providing old data back to the client.
        if not isinstance(unpickled, raft.messages.GetMessage):
            # send request to raft network, in case node is not leader, it can return the leader_id
            # if it has any, and then it sends `LeaderInfo` message back to the client
            event_or_leader_id = self._raft_controller.handle_client_request(unpickled)

            if isinstance(event_or_leader_id, asyncio.Event):
                await event_or_leader_id.wait()
                response = self.apply(unpickled)
            else:
                response = LeaderInfo(event_or_leader_id)
        else:
            response = self.apply(unpickled)

        # respond to client
        await transport.send_message(writer, pickle.dumps(response))

    def apply(self, message: raft.messages.BaseClientMessage) -> Response:
        """
        This is called by KVStore itself when responding to client, or it can be also
        directly called by FOLLOWER when applying state.
        """
        print(f'Applying {message}')
        if isinstance(message, raft.messages.SetMessage):
            response = self._set_request(message.key, message.value)
        elif isinstance(message, raft.messages.GetMessage):
            response = self._get_request(message.key)
        elif isinstance(message, raft.messages.DeleteMessage):
            response = self._delete_request(message.key)
        else:
            response = Response(ResponseEnum.OK.name)
        return response

    def _delete_request(self, key: str) -> Response:
        """
        Delete key from store.
        """

        try:
            del self._store[key]
        except KeyError:
            return Response(ResponseEnum.NOT_OK.name)
        else:
            return Response(ResponseEnum.OK.name)

    def _set_request(self, key: str, value: Any) -> Response:
        """
        Set key to value in store.
        """

        self._store[key] = value
        return Response(ResponseEnum.OK.name)

    def _get_request(self, key: str) -> Union[Response, Union[str, int]]:
        """
        Returns value of key in store.
        """

        try:
            return self._store[key]
        except KeyError:
            return Response(ResponseEnum.NOT_OK.name)
