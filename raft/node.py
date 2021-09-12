from typing import Dict, Tuple, Optional, Callable

import asyncio
import enum
import logging
import pickle

import raft.log
import raft.messages


class State(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftNode:

    """
    Raft node encompassing the main RAFT logic.

    Can be of 3 `State` states:

    LEADER: Sends append entry messages to the rest of the raft network in order to replicate
            all of his log entries to logs of other nodes. Also leader is the only node, which
            client can talk to.

    FOLLOWER: Starting state. Listens to append entries from leader and has no information about
              the state of other nodes.

    CANDIDATE: Whenever election timeout occurs node switched to candidate position, requesting
               to become new leader. Election timeout can occur at the start of the raft network
               when there was not previous leader, or if f.e. previous leader has crashed.
    """

    def __init__(
        self,
        index: int,
        event_box: asyncio.Queue,
        nodes: Dict[int, Tuple[str, int]],
        state_machine_callback: Callable[
            ['raft.messages.BaseClientMessage'], 'raft.kvserver.Response'
        ],
        persistence=False,
    ):
        self._index = index
        self._event_box = event_box
        self._state = State.FOLLOWER
        self._nodes = [index for index in nodes]
        self._leader_id = None
        self._state_machine_callback = state_machine_callback
        self._heard_from_leader = False
        self._votes_granted = set()
        self._client_requests: Dict[int, asyncio.Event] = {}

        ##
        # Volatile state
        ##

        # index of highest log entry known to be committed
        self._commit_index = -1
        # index of highest log entry applied to state machine
        self._last_applied = None
        # for each server, index of highest log entry known to be replicated on server
        self._match_index: Optional[Dict[int, int]] = None
        # for each server, index of the next log entry to send to that server
        self._next_index: Optional[Dict[int, int]] = None

        ##
        # Persistent state
        ##

        # candidate index that received vote in current term (or None if not voted)
        self._voted_for = None
        # latest term node has seen
        self._current_term = 0
        # log entries, each entry contains command for state machine and term when entry
        # was received by leader
        self._log = raft.log.Log()

        self._persisted_state_filename = f'node-{self._index}.state'
        self._logger = logging.getLogger(
            f'{self.__class__.__name__}<index:{self._index}>'
        )
        self._persistence = persistence
        if self._persistence:
            self._read_persisted_state()

    ##
    # Public
    ##

    @property
    def is_leader(self) -> bool:
        return self._state is State.LEADER

    def get_current_leader(self) -> Optional[int]:
        return self._leader_id

    def handle_heartbeat(self) -> None:
        """
        Callback for expired heartbeat timer.

        Sends append entries to the rest of the nodes.
        """
        self._send_append_entries()

    def handle_election_timeout(self) -> None:
        """
        Callback for expired election timeout.

        Sends request votes in case node is not leader and haven't heard from leader.
        """

        if not self.is_leader and not self._heard_from_leader:
            self._become_candidate()
        self._heard_from_leader = False

    def handle_message(self, message: raft.messages.NodeMessage) -> None:
        """
        Message handler. Used as callback for `RaftProtocol` and called on every incoming message.
        """

        if message.term > self._current_term:
            self._become_follower()
            self._current_term = message.term

        if isinstance(message, raft.messages.AppendEntries):
            self._handle_append_entry(message)
        elif isinstance(message, raft.messages.AppendEntriesResponse):
            self._handle_append_entry_response(message)
        elif isinstance(message, raft.messages.RequestVote):
            self._handle_request_vote(message)
        elif isinstance(message, raft.messages.RequestVoteResponse):
            self._handle_request_vote_response(message)

    def handle_client_request(
        self, message: raft.messages.BaseClientMessage
    ) -> asyncio.Event:
        """
        Callback for handling client requests coming from the application layer (KVStore).

        Node tries to replicate the message (command/request) in the majority of the other nodes.
        If it succeeds, it informs the `KVStore` that the message can be applied to it's state.
        """

        client_request_event = asyncio.Event()
        self._logger.info('Handling client request - %s', message)
        log_entry = raft.log.LogEntry(self._current_term, message)

        # append to own log
        self._log.append_entries(
            previous_index=len(self._log) - 1,
            previous_log_term=self._log.get_previous_term(len(self._log) - 1),
            entries=[log_entry],
        )

        self._persist_state()

        self._match_index[self._index] += 1
        self._next_index[self._index] += 1

        self._client_requests[len(self._log) - 1] = client_request_event
        return client_request_event

    ##
    # Private
    ##

    def _send(self, destination_index: int, message: raft.messages.NodeMessage) -> None:
        """
        Method for passing messages to other nodes in the raft network. It doesn't actually
        sends the message, but rather just put it in the `self._event_box` queue, which is
        then handled by controller.
        """

        self._logger.debug('Sending message to %s - %s', destination_index, message)
        self._event_box.put_nowait((destination_index, message))

    def _send_request_votes(self) -> None:
        """
        Send `RequestVote` message to the rest of the raft nodes.
        """

        for node_index in self._nodes:

            if node_index == self._index:
                continue

            request_vote = raft.messages.RequestVote(
                sender_index=self._index,
                term=self._current_term,
                last_log_index=len(self._log) - 1,
                last_log_term=self._log[-1].term if self._log else -1,
            )
            self._send(node_index, request_vote)

    def _send_append_entries(self) -> None:
        """
        Send `AppendEntries` message to the rest of the raft nodes.
        """

        for node_index in self._nodes:

            if node_index == self._index:
                continue

            append_entry = self._create_append_entry_message(node_index)
            self._send(node_index, append_entry)

    def _create_append_entry_message(
        self, node_index: int
    ) -> raft.messages.AppendEntries:
        """
        Create `AppendEntries` message specifically tailored for raft node with `node_index`.
        """

        entries = self._log[self._next_index[node_index] :]
        append_entry = raft.messages.AppendEntries(
            sender_index=self._index,
            term=self._current_term,
            leader_id=self._index,
            commit_index=self._commit_index,
            previous_index=self._next_index[node_index] - 1,
            previous_term=self._log.get_previous_term(self._next_index[node_index] - 1),
            entries=entries,
        )
        return append_entry

    def _handle_append_entry(self, message: raft.messages.AppendEntries):
        """
        Method for handling `AppendEntries` message from leader. Need to handle also some edge
        cases, f.e. we can receive append entry message from old leader that is not aware of
        new election etc..
        """

        if message.term < self._current_term:
            success = False
        else:
            if self._state is State.CANDIDATE:
                self._become_follower()

            self._leader_id = message.leader_id
            self._heard_from_leader = True

            if success := self._log.append_entries(
                previous_index=message.previous_index,
                previous_log_term=message.previous_term,
                entries=message.entries,
            ):
                if message.commit_index > self._commit_index:
                    new_commit_index = min(
                        message.commit_index,
                        message.previous_index + len(message.entries),
                    )
                    for c_index in range(self._commit_index + 1, new_commit_index + 1):
                        self._state_machine_callback(self._log[c_index].entry)
                        self._last_applied = c_index
                    self._commit_index = new_commit_index

        response = raft.messages.AppendEntriesResponse(
            sender_index=self._index,
            term=self._current_term,
            match_index=message.previous_index + len(message.entries),
            success=success,
        )

        self._send(message.sender_index, response)

    def _handle_append_entry_response(
        self, message: raft.messages.AppendEntriesResponse
    ) -> None:
        """
        Leader method for handling append entry response
        """

        assert self.is_leader

        node_index = message.sender_index

        if message.success:
            self._match_index[node_index] = max(
                message.match_index, self._match_index[node_index]
            )
            self._next_index[node_index] = self._match_index[node_index] + 1
            if (
                new_commit_index := sorted(self._match_index.values())[
                    len(self._nodes) // 2
                ]
            ) != self._commit_index:

                # leader cannot commit entries from previous leaders, it can happen only in case
                # he also commits at least one index from his own term
                if self._log[new_commit_index].term < self._current_term:
                    return

                for c_index in range(self._commit_index + 1, new_commit_index + 1):
                    self._last_applied = c_index
                    self._client_requests[c_index].set()
                    del self._client_requests[c_index]
                self._commit_index = new_commit_index

        else:
            self._next_index[node_index] = self._next_index[node_index] - 1
            self._send(node_index, self._create_append_entry_message(node_index))

    def _handle_request_vote(self, message: raft.messages.RequestVote) -> None:
        """
        Handle request vote message from leader
        """

        response = raft.messages.RequestVoteResponse(
            term=self._current_term, sender_index=self._index, vote_granted=True
        )

        # Deny the request vote for old term, and also if the node has already voted for someone else
        if message.term < self._current_term or (
            self._voted_for is not None and self._voted_for != message.sender_index
        ):
            response.vote_granted = False

        else:

            last_log_index = len(self._log) - 1
            last_log_term = self._log[-1].term if self._log else -1

            # Deny if the term of the last log is older then ours or, in case both terms
            # are the same, then we deny for candidates with shorter log than ours
            if last_log_term > message.last_log_term or (
                last_log_term == message.last_log_term
                and last_log_index > message.last_log_index
            ):
                response.vote_granted = False

        if response.vote_granted:
            self._heard_from_leader = True
            self._voted_for = message.sender_index

        self._send(message.sender_index, response)

    def _handle_request_vote_response(
        self, message: raft.messages.RequestVoteResponse
    ) -> None:
        """
        Candidate's method for handling request vote response.
        """
        if message.term < self._current_term:
            return

        if message.vote_granted:
            self._votes_granted.add(message.sender_index)
            if self._state is State.CANDIDATE and len(self._votes_granted) > (
                len(self._nodes) // 2
            ):
                self._become_leader()

    def _become_leader(self) -> None:
        """
        Set `LEADER` state after winning election.

        Newly elected leader has no idea in what state are logs of other nodes, so we
        initialize `self._match_index` to -1 index.
        """

        self._leader_id = self._index
        self._state = State.LEADER
        self._match_index = {node_index: -1 for node_index in self._nodes}
        self._match_index[self._index] = len(self._log) - 1
        self._next_index = {x: len(self._log) for x in self._nodes}

    def _become_follower(self) -> None:
        """
        Set `FOLLOWER` state
        """

        self._state = State.FOLLOWER
        self._client_requests.clear()
        self._voted_for = None

    def _become_candidate(self) -> None:
        """
        Set state to `CANDIDATE` and send request vote messages to the rest of the raft nodes.
        """

        self._current_term += 1
        self._state = State.CANDIDATE
        self._voted_for = self._index
        self._votes_granted.add(self._index)
        self._send_request_votes()

    def _read_persisted_state(self) -> None:
        """
        Load persisted state from disk.
        """

        try:
            with open(self._persisted_state_filename, 'rb') as f:
                binary_state = f.read()
                unpickled = pickle.loads(binary_state)
                self._log = unpickled['log']
                self._voted_for = unpickled['voted_for']
                self._current_term = unpickled['current_term']
        except FileNotFoundError:
            return

    def _persist_state(self) -> None:
        """
        Persist state on disk.
        """

        if not self._persistence:
            return
        with open(self._persisted_state_filename, 'wb') as f:
            state = {
                # TODO: this is bad as we don't want to write the full log on disk every time there is a change
                'log': self._log,
                'voted_for': self._voted_for,
                'current_term': self._current_term,
            }
            pickled = pickle.dumps(state)
            f.write(pickled)
