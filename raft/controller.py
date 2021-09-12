from typing import Optional, Callable, Union

import asyncio
import contextlib
import random

import raft.node
import raft.network
import raft.settings
import raft.manhole
import raft.messages


class RaftController:

    """
    Control unit for raft node.

    Controller has 3 main purposes:

    1. Create link between communication protocol and raft node

    2. Create link between state machine and raft node through `self.handle_client_request` which is called
       by the state machine whenever a client request arrives.

    3. Run heartbeat and election timeout timers for the raft node. Whenever timer expires, corresponding
       method from raft node is called. Raft node can then respond to expired timer by putting an event into
       `self._node_event_box`, which is awaited and handled by controller.

    This design decision was to mainly keep all the I/O logic and operations away from the raft node.
    """

    HEARTBEAT_TIMER = 1
    ELECTION_TIMEOUT = HEARTBEAT_TIMER * 5
    ELECTION_TIMEOUT_RANDOM = HEARTBEAT_TIMER * 2

    def __init__(
        self,
        index: int,
        state_machine_callback: Callable[
            [raft.messages.BaseClientMessage], 'raft.kvserver.Response'
        ],
    ):
        self._index = index
        self._node_event_box = asyncio.Queue()
        self._raft_node = raft.node.RaftNode(
            index=index,
            event_box=self._node_event_box,
            nodes=raft.settings.RAFT_NODES,
            state_machine_callback=state_machine_callback,
        )
        self._protocol = raft.network.RaftProtocol(
            node_id=self._index,
            raft_nodes=raft.settings.RAFT_NODES,
            on_message_callback=self._raft_node.handle_message,
        )
        self._check_node_events_task: Optional[asyncio.Task] = None
        self._heartbeat_timer_task: Optional[asyncio.Task] = None
        self._election_timeout_timer_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._start_manhole()
        await self._protocol.start()
        self._check_node_events_task = asyncio.create_task(self._check_node_events())
        self._heartbeat_timer_task = asyncio.create_task(self._heartbeat_timer())
        self._election_timeout_timer_task = asyncio.create_task(
            self._election_timeout_timer()
        )

    async def stop(self) -> None:
        await self._protocol.stop()
        self._check_node_events_task.cancel()
        self._heartbeat_timer_task.cancel()
        self._election_timeout_timer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._check_node_events_task
            await self._heartbeat_timer_task
            await self._election_timeout_timer_task

    def handle_client_request(
        self, message: raft.messages.BaseClientMessage
    ) -> Union[int, asyncio.Event]:
        """
        Method called by state machine (KVServer) on client request. Message is further handled
        by the raft node itself and `asyncio.Event` is returned, which is set when the message
        has been commited. When event is set, KVServer is free to respond to client.

        In case raft node is not actually the leader it returns the ID of the current leader instead.
        """

        # clients only talk to leader
        if not self._raft_node.is_leader:
            return self._raft_node.get_current_leader()
        return self._raft_node.handle_client_request(message)

    async def _check_node_events(self) -> None:
        """
        Wait for an event (message) from raft node and pass it to protocol.
        """

        while True:
            destination_index, message = await self._node_event_box.get()
            await self._protocol.send_message(
                message=message, server_id=destination_index
            )

    async def _election_timeout_timer(self) -> None:
        """
        Alert raft node when election timer expires. Election timeout time is randomized on every
        cycle so that we avoid a situation where multiple nodes would have the same election timeouts.
        """

        while True:
            await asyncio.sleep(
                self.ELECTION_TIMEOUT + random.randrange(self.ELECTION_TIMEOUT_RANDOM)
            )
            self._raft_node.handle_election_timeout()

    async def _heartbeat_timer(self) -> None:
        """
        Alert raft node on expired heartbeat timer. If raft node is not leader this only sleeps.
        """

        while True:
            await asyncio.sleep(self.HEARTBEAT_TIMER)
            if self._raft_node.is_leader:
                self._raft_node.handle_heartbeat()

    def _start_manhole(self) -> None:
        namespace = {
            'controller': self,
            'node': self._raft_node,
        }

        raft.manhole.start_manhole(
            host=raft.settings.AIOMANHOLE_HOST,
            port=raft.settings.AIOMANHOLE_PORT - self._index,
            socket_path=raft.settings.AIOMANHOLE_SOCKET,
            namespace=namespace,
        )
