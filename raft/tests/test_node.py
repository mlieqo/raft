import pytest
import unittest.mock

import raft.log
import raft.node
import raft.settings
import raft.messages


@pytest.fixture
def node():
    event_box = unittest.mock.MagicMock(put_nowait=unittest.mock.Mock())
    state_machine_callback = unittest.mock.MagicMock()
    log = (1, 1, 1, 2, 3)
    node = raft.node.RaftNode(
        index=0,
        event_box=event_box,
        nodes=raft.settings.RAFT_NODES,
        state_machine_callback=state_machine_callback,
    )
    node._current_term = 3
    node._log = raft.log.Log([raft.log.LogEntry(x, 'msg') for x in log])
    return node


def test_become_follower(node):
    node._state = raft.node.State.LEADER
    node._voted_for = 2
    node._become_follower()
    assert node._state is raft.node.State.FOLLOWER
    assert node._voted_for is None


def test_become_candidate(node):
    node._become_candidate()
    assert node._current_term == 4
    assert node._state is raft.node.State.CANDIDATE
    assert node._voted_for == node._index
    assert node._votes_granted == {node._index}
    request_vote = raft.messages.RequestVote(
        sender_index=node._index,
        term=node._current_term,
        last_log_index=len(node._log) - 1,
        last_log_term=node._log[-1].term if node._log else -1,
    )
    calls = [(x, request_vote) for x in range(1, 5)]
    node._event_box.put_nowait.has_calls(calls)


@pytest.mark.parametrize(
    'responses,expected_result',
    [
        (
            (
                raft.messages.RequestVoteResponse(
                    sender_index=1, term=5, vote_granted=False
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=2, term=4, vote_granted=True
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=3, term=4, vote_granted=True
                ),
            ),
            {'_state': raft.node.State.FOLLOWER},
        ),
        (
            (
                raft.messages.RequestVoteResponse(
                    sender_index=1, term=4, vote_granted=False
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=2, term=4, vote_granted=True
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=3, term=4, vote_granted=False
                ),
            ),
            {'_state': raft.node.State.CANDIDATE},
        ),
        (
            (
                raft.messages.RequestVoteResponse(
                    sender_index=1, term=4, vote_granted=True
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=2, term=4, vote_granted=True
                ),
                raft.messages.RequestVoteResponse(
                    sender_index=3, term=4, vote_granted=False
                ),
            ),
            {'_state': raft.node.State.LEADER},
        ),
    ],
)
def test_handle_request_vote_response(node, responses, expected_result):
    node._become_candidate()

    for response in responses:
        node.handle_message(response)

    for parameter, value in expected_result.items():
        assert getattr(node, parameter) == value


@pytest.mark.parametrize(
    'extra_attributes,request_,expected_result',
    [
        (
            {},
            raft.messages.RequestVote(
                sender_index=1, term=3, last_log_index=4, last_log_term=2
            ),
            {'vote_granted': False, 'term': 3},
        ),
        (
            {'_voted_for': 3, '_current_term': 4},
            raft.messages.RequestVote(
                sender_index=1, term=4, last_log_index=4, last_log_term=3
            ),
            {'vote_granted': False, 'term': 4},
        ),
        (
            {'_voted_for': None, '_current_term': 4},
            raft.messages.RequestVote(
                sender_index=1, term=4, last_log_index=4, last_log_term=3
            ),
            {'vote_granted': True, 'term': 4},
        ),
        (
            {'_voted_for': 4, '_current_term': 3},
            raft.messages.RequestVote(
                sender_index=1, term=4, last_log_index=4, last_log_term=3
            ),
            {'vote_granted': True, 'term': 4},
        ),
        (
            {},
            raft.messages.RequestVote(
                sender_index=1, term=4, last_log_index=4, last_log_term=2
            ),
            {'vote_granted': False, 'term': 4},
        ),
        (
            {},
            raft.messages.RequestVote(
                sender_index=1, term=4, last_log_index=2, last_log_term=3
            ),
            {'vote_granted': False, 'term': 4},
        ),
    ],
)
def test_handle_request_vote(node, extra_attributes, request_, expected_result):
    for attr, value in extra_attributes.items():
        setattr(node, attr, value)

    node.handle_message(request_)
    response = raft.messages.RequestVoteResponse(
        0, expected_result['term'], expected_result['vote_granted']
    )
    node._event_box.put_nowait.assert_called_with((1, response))


def test_successful_append_entry_response(node):
    import asyncio

    message = raft.messages.AppendEntriesResponse(1, 3, 4, True)
    node._current_term = 3
    node._commit_index = 0
    node._state = raft.node.State.LEADER
    node._match_index = {0: 4, 1: 0, 2: 4, 3: 0, 4: 0}
    node._next_index = {0: 5, 1: 1, 2: 5, 3: 1, 4: 1}
    events = [asyncio.Event() for _ in range(1, 5)]
    node._client_requests = {x + 1: events[x] for x in range(0, 4)}
    node.handle_message(message)
    assert node._commit_index == 4
    for e in events:
        assert e.is_set()

    message = raft.messages.AppendEntriesResponse(1, 3, 4, True)
    node._current_term = 3
    node._commit_index = 0
    node._state = raft.node.State.LEADER
    node._match_index = {0: 4, 1: 0, 2: 2, 3: 0, 4: 0}
    node._next_index = {0: 5, 1: 1, 2: 3, 3: 1, 4: 1}
    node._client_requests = {1: (e := asyncio.Event())}
    node.handle_message(message)
    assert node._commit_index == 0
    assert not e.is_set()


@pytest.mark.parametrize(
    'request_,expected_result,attributes',
    [
        (
            raft.messages.AppendEntries(
                sender_index=1,
                term=2,
                commit_index=2,
                leader_id=1,
                previous_index=4,
                previous_term=3,
                entries=[raft.log.LogEntry(4, 'ola')],
            ),
            {'success': False, 'term': 3},
            {},
        ),
        (
            raft.messages.AppendEntries(
                sender_index=1,
                term=4,
                commit_index=4,
                leader_id=1,
                previous_index=4,
                previous_term=3,
                entries=[raft.log.LogEntry(4, 'ola')],
            ),
            {'success': True, 'term': 4},
            {'_commit_index': 4},
        ),
    ],
)
def test_append_entry(node, attributes, request_, expected_result):
    node.handle_message(request_)
    response = raft.messages.AppendEntriesResponse(
        0, expected_result['term'], match_index=5, success=expected_result['success']
    )
    for attr, value in attributes.items():
        assert getattr(node, attr) == attributes[attr]
    node._event_box.put_nowait.assert_called_with((1, response))
