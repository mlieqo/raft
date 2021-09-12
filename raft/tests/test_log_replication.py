"""
Figures can be found in raft paper: https://raft.github.io/raft.pdf
"""
import asyncio
import pytest

import raft.controller
import raft.log
import raft.messages
import raft.settings


def create_log_entry(term, entry):
    return raft.log.LogEntry(term, entry)


@pytest.mark.asyncio
async def test_figure_6(nodes):
    n0 = (1, 1, 1, 2, 3, 3, 3, 3)
    n1 = (1, 1, 1, 2, 3)
    n2 = (1, 1, 1, 2, 3, 3, 3, 3)
    n3 = (1, 1)
    n4 = (1, 1, 1, 2, 3, 3, 3)

    nodes[0]._log._log = [create_log_entry(x, 'msg') for x in n0]
    nodes[1]._log._log = [create_log_entry(x, 'msg') for x in n1]
    nodes[2]._log._log = [create_log_entry(x, 'msg') for x in n2]
    nodes[3]._log._log = [create_log_entry(x, 'msg') for x in n3]
    nodes[4]._log._log = [create_log_entry(x, 'msg') for x in n4]

    nodes[0]._become_leader()
    nodes[0]._current_term = 3
    nodes[1]._current_term = 3
    nodes[2]._current_term = 3
    nodes[3]._current_term = 1
    nodes[4]._current_term = 3

    await asyncio.sleep(1)
    assert (
        nodes[0]._log
        == nodes[1]._log
        == nodes[2]._log
        == nodes[3]._log
        == nodes[4]._log
    )


@pytest.mark.asyncio
async def test_figure_7(nodes):
    n0 = (1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
    n1 = (1, 1, 1, 4, 4, 4, 4)
    n2 = (1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3)
    n3 = (1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6)
    n4 = (1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7)

    nodes[0]._log._log = [create_log_entry(x, 'msg') for x in n0]
    nodes[1]._log._log = [create_log_entry(x, 'msg') for x in n1]
    nodes[2]._log._log = [create_log_entry(x, 'msg') for x in n2]
    nodes[3]._log._log = [create_log_entry(x, 'msg') for x in n3]
    nodes[4]._log._log = [create_log_entry(x, 'msg') for x in n4]

    nodes[0]._become_leader()
    nodes[0]._current_term = 8
    nodes[1]._current_term = 6
    nodes[2]._current_term = 4
    nodes[3]._current_term = 4
    nodes[4]._current_term = 3
    msg = raft.messages.BaseClientMessage('ola')
    nodes[0].handle_client_request(msg)

    await asyncio.sleep(1)
    assert (
        nodes[0]._log
        == nodes[1]._log
        == nodes[2]._log
        == nodes[3]._log
        == nodes[4]._log
    )
