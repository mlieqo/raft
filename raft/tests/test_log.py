import pytest

import raft.log


def create_log_entry(term):
    return raft.log.LogEntry(term, 'test log')


LOG_ENTRIES = [
    create_log_entry(1),
    create_log_entry(1),
    create_log_entry(2),
    create_log_entry(2),
    create_log_entry(3),
]


@pytest.fixture
def empty_log():
    return raft.log.Log()


@pytest.fixture
def populated_log(empty_log):
    empty_log.append_entries(-1, 1, LOG_ENTRIES)
    return empty_log


@pytest.mark.parametrize(
    'append_entry_input',
    [
        (5, 5, [create_log_entry(6)]),
        (5, 6, [create_log_entry(7)]),
        (5, 4, [create_log_entry(9)]),
        (5, 0, [create_log_entry(10)]),
    ],
)
def test_append_entry_wrong_term(populated_log, append_entry_input):
    # fail because previous term was 3
    assert not populated_log.append_entries(*append_entry_input)


@pytest.mark.parametrize(
    'append_entry_input',
    [
        (6, 4, [create_log_entry(1)]),
        (7, 4, [create_log_entry(1)]),
    ],
)
def test_index_too_big(populated_log, append_entry_input):
    # last filled index is 4
    assert not populated_log.append_entries(*append_entry_input)


def test_insert_out_of_order(populated_log):
    log_entry = [create_log_entry(1000), create_log_entry(1001)]
    print(log_entry)
    assert populated_log.append_entries(1, 1, log_entry)
    assert populated_log._log == LOG_ENTRIES[:2] + log_entry


@pytest.mark.parametrize(
    'append_entry_input',
    [
        (4, 3, [create_log_entry(3), create_log_entry(3)]),
        (4, 3, [create_log_entry(1000)]),
        (4, 3, [create_log_entry(5), create_log_entry(6), create_log_entry(7)]),
    ],
)
def test_append(populated_log, append_entry_input):
    assert populated_log.append_entries(*append_entry_input)
    assert populated_log._log == LOG_ENTRIES + append_entry_input[2]
