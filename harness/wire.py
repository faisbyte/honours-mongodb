"""What each client actually put on the wire, from PyMongo command monitoring.

The causal-session bug went unnoticed because nothing recorded what the
driver sent: the harness advanced the session and assumed afterClusterTime
followed. A CommandListener on every client removes the assumption. For each
read the collector records the readConcern level and afterClusterTime exactly
as they appeared in the find command, and a causal target that sends a read
without afterClusterTime aborts the run.

PyMongo publishes command events synchronously, on the thread that runs the
operation. Per-read state is therefore kept thread-local, which ties each event
to the read that caused it with no locking on the hot path. Per-client totals
are kept under a lock and go into the manifest.
"""

from __future__ import annotations

import collections
import threading

from pymongo import monitoring


def ts_doc(ts) -> dict | None:
    """bson Timestamp to the {t, i} form used throughout the raw records."""
    return {"t": ts.time, "i": ts.inc} if ts is not None else None


class WireListener(monitoring.CommandListener):

    def __init__(self, name: str) -> None:
        self.name = name
        self._local = threading.local()
        self._lock = threading.Lock()
        self._commands: collections.Counter = collections.Counter()
        self._read_concerns: collections.Counter = collections.Counter()

    # -- per read ----------------------------------------------------------

    def begin(self) -> None:
        """Forget the previous read. Call immediately before each read."""
        self._local.sent = None

    def last_find(self) -> dict | None:
        """The find this thread sent since begin(), or None if none was sent.

        {"request_id", "read_concern": level or None,
         "after_cluster_time": {t, i} or None}
        """
        return getattr(self._local, "sent", None)

    # -- CommandListener ---------------------------------------------------

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        rc = event.command.get("readConcern") or {}
        if event.command_name == "find":
            self._local.sent = {
                "request_id": event.request_id,
                "read_concern": rc.get("level"),
                "after_cluster_time": ts_doc(rc.get("afterClusterTime")),
            }
        with self._lock:
            self._commands[event.command_name] += 1
            if rc or event.command_name == "find":
                act = "yes" if "afterClusterTime" in rc else "no"
                self._read_concerns[f"{event.command_name} level="
                                    f"{rc.get('level')} afterClusterTime={act}"] += 1

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        pass

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        pass

    # -- manifest ----------------------------------------------------------

    def summary(self) -> dict:
        with self._lock:
            return {"client": self.name,
                    "commands": dict(self._commands),
                    "read_concerns": dict(self._read_concerns)}


def causal_problem(sent: dict | None, expected_act: bool,
                   error: str | None) -> str | None:
    """Why a causal target's read does not count as causal, or None.

    expected_act: the session held an operationTime before the read, so the
    driver had an afterClusterTime to send. A read that failed before reaching
    the wire (error and nothing sent) is an ordinary error, not a violation.
    """
    if sent is None:
        return None if error else "no find command was observed on the wire"
    if expected_act and sent["after_cluster_time"] is None:
        return "find was sent without afterClusterTime"
    return None
