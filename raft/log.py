from typing import List

import dataclasses


@dataclasses.dataclass
class LogEntry:
    term: int
    entry: 'raft.messages.BaseClientMessage'

    def __repr__(self):
        return f'LogEntry <term:{self.term}> <entry:{self.entry}>'


class Log:
    """
    Raft replicated log.

    Contains log entries (`self._log`), each entry consists of command for state machine, and term when entry
    was received by leader.
    """

    def __init__(self, log=None):
        self._log = log if log else []

    def __len__(self):
        return len(self._log)

    def __repr__(self):
        return f'Logs - {self._log}'

    def __getitem__(self, index: int) -> LogEntry:
        return self._log[index]

    def __eq__(self, other: 'Log') -> bool:
        return isinstance(other, Log) and self._log == other._log

    def get_previous_term(self, previous_index: int) -> int:
        try:
            return self._log[previous_index].term
        except IndexError:
            return -1

    def append_entries(
        self, previous_index: int, previous_log_term: int, entries: List[LogEntry]
    ) -> bool:
        """
        Add new entries to the log and return success.

        `previous_index` is the index of the log entry that precedes the new entries
        `previous_log_term` is the term number of the log entry that precedes new entries
        `entries` list of LogEntry objects
        """

        # missing log entries - cannot create gaps in log
        if previous_index >= len(self._log):
            return False

        # if previous log entry term doesn't match previous term supplied, we cannot append as it
        # violates property:
        #
        #   "If two entries in different logs have the same index and term,
        #    then the logs are identical in all preceding entries"
        elif (
            previous_index >= 0 and previous_log_term != self._log[previous_index].term
        ):
            return False

        # "If and existing entry conflicts with a new one (same index, but different terms) delete
        # the existing entry and all that follow it"
        for index, entry in enumerate(entries, previous_index + 1):
            if index < len(self._log) and self._log[index].term != entry.term:
                del self._log[index:]
                break

        # add entries
        self._log[previous_index + 1 : previous_index + 1 + len(entries)] = entries

        return True
