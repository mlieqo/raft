import asyncio
import pytest
import unittest.mock

import raft.controller
import raft.messages


def create_controller(node_index):
    kvserver = unittest.mock.MagicMock(
        apply=unittest.mock.Mock(),
    )
    return raft.controller.RaftController(node_index, kvserver)


@pytest.fixture
async def controllers(monkeypatch):
    controller_0 = create_controller(0)
    controller_1 = create_controller(1)

    monkeypatch.setattr(
        controller_1._raft_node,
        'handle_message',
        unittest.mock.Mock(side_effect=controller_1._raft_node.handle_message),
    )
    monkeypatch.setattr(
        controller_1._protocol,
        '_on_message_callback',
        unittest.mock.Mock(side_effect=controller_1._raft_node.handle_message),
    )

    monkeypatch.setattr(
        controller_0._raft_node,
        'handle_message',
        unittest.mock.Mock(side_effect=controller_0._raft_node.handle_message),
    )
    monkeypatch.setattr(
        controller_0._protocol,
        '_on_message_callback',
        unittest.mock.Mock(side_effect=controller_0._raft_node.handle_message),
    )

    await controller_0.start()
    await controller_1.start()
    await asyncio.sleep(0.5)

    yield controller_0, controller_1

    await controller_0.stop()
    await controller_1.stop()


@pytest.mark.asyncio
async def test_send_message(controllers):
    controller_0, controller_1 = controllers
    message = raft.messages.NodeMessage(controller_0._index, 1)
    controller_0._raft_node._send(1, message)

    await asyncio.sleep(0.1)
    controller_1._raft_node.handle_message.assert_called_with(message)


@pytest.mark.asyncio
async def test_send_receive(controllers):
    controller_0, controller_1 = controllers

    message = raft.messages.AppendEntries(controller_0._index, 1, 0, -1, -1, -1, [])
    controller_0._raft_node._send(1, message)

    await asyncio.sleep(0.1)
    controller_1._raft_node.handle_message.assert_called_with(message)
    await asyncio.sleep(0.1)
    controller_0._raft_node.handle_message.assert_called()
