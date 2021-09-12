import asyncio
import pytest

import raft.log


def create_log_entry(term, entry):
    return raft.log.LogEntry(term, entry)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'index,expected_result',
    [
        (0, True),
        (1, False),
        (2, False),
        (3, True),
        (4, True),
    ],
)
async def test_leader_election_logic(index, expected_result, nodes):

    log_dict = {
        0: (1, 1, 1, 4, 4, 5, 5, 6, 6, 6),
        1: (1, 1, 1, 4, 4, 4, 4),
        2: (1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3),
        3: (1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6),
        4: (1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7),
    }

    for i, entries in log_dict.items():
        nodes[i]._log = [create_log_entry(x, 'msg') for x in entries]
        nodes[i]._current_term = entries[-1]

    nodes[index]._become_candidate()
    await asyncio.sleep(0.2)
    assert nodes[index].is_leader is expected_result
