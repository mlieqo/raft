import asyncio
import unittest.mock
import pytest

import raft.log
import raft.controller


def create_controller(node_index):
    kvserver = unittest.mock.MagicMock(apply=unittest.mock.Mock())
    return raft.controller.RaftController(node_index, kvserver)


@pytest.fixture
async def controllers(monkeypatch):
    contrls = [create_controller(x) for x in range(5)]
    contrls[0].HEARTBEAT_TIMER = 0.1
    for c in contrls:
        await c.start()

    # startup time
    await asyncio.sleep(0.5)
    yield contrls

    for c in contrls:
        await c.stop()


@pytest.fixture
def nodes(controllers):
    # shortcut to nodes
    return [c._raft_node for c in controllers]
