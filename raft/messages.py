from typing import List
import dataclasses

import raft.log


@dataclasses.dataclass
class NodeMessage:
    sender_index: int
    term: int


###
# APPEND ENTRY
###


@dataclasses.dataclass
class AppendEntries(NodeMessage):
    # maybe redundant, because of `sender_id`?
    leader_id: int
    commit_index: int
    previous_index: int
    previous_term: int
    entries: List[raft.log.LogEntry]


@dataclasses.dataclass
class AppendEntriesResponse(NodeMessage):
    # this is for leader to identify to which request it is response
    match_index: int
    success: bool


###
# REQUEST VOTE
###


@dataclasses.dataclass
class RequestVote(NodeMessage):
    last_log_index: int
    last_log_term: int


@dataclasses.dataclass
class RequestVoteResponse(NodeMessage):
    vote_granted: bool


###
# CLIENT MESSAGES
###


@dataclasses.dataclass
class BaseClientMessage:
    key: str


@dataclasses.dataclass
class SetMessage(BaseClientMessage):
    value: str


@dataclasses.dataclass
class GetMessage(BaseClientMessage):
    pass


@dataclasses.dataclass
class DeleteMessage(BaseClientMessage):
    pass
