"""Bounded, read-only diagnostics from the scheduler's existing worker heartbeats."""

import logging
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)
DEFAULT_DIAGNOSTICS_INTERVAL_SECONDS = 300
_DIAGNOSTICS_TIMEOUT_SECONDS = 10


def _worker_snapshot(dask_scheduler):
    """Read O(workers) state without scanning tasks, files, or querying workers."""
    result = []
    for address, worker in dask_scheduler.workers.items():
        memory = worker.memory
        result.append(
            {
                "address": address,
                "host": worker.host,
                "local_directory": worker.local_directory,
                "memory_limit": worker.memory_limit,
                "rss": memory.process,
                "managed": memory.managed,
                "unmanaged": memory.unmanaged,
                "unmanaged_recent": memory.unmanaged_recent,
                "spill_disk": worker.metrics.get("spilled_bytes", {}).get("disk", 0),
                "spill_memory": worker.metrics.get("spilled_bytes", {}).get("memory", 0),
                "processing": len(worker.processing),
                "status": str(worker.status),
            }
        )
    return result


class _ClusterDiagnostics:
    def __init__(self, client):
        self.client = client
        self.directories = set()
        self.previous_workers = None

    def report(self):
        """Log totals and the most memory-pressured worker, tolerating RPC failures."""
        try:
            # The synchronous public wrapper does not expose an RPC deadline.
            # Use its async implementation through sync's bounded wait, on the
            # existing client's own loop (no extra client or event-loop mixing).
            workers = self.client.sync(
                self.client._run_on_scheduler,
                _worker_snapshot,
                callback_timeout=_DIAGNOSTICS_TIMEOUT_SECONDS,
            )
            current = {worker["address"] for worker in workers}
            if self.previous_workers is not None and current != self.previous_workers:
                logger.info(
                    "Dask diagnostics: workers added=%d removed=%d",
                    len(current - self.previous_workers),
                    len(self.previous_workers - current),
                )
            self.previous_workers = current
            for worker in workers:
                location = (worker["address"], worker["local_directory"])
                if location not in self.directories:
                    logger.info(
                        "Dask worker %s host=%s spill/local_directory=%s memory_limit=%.2f GiB",
                        worker["address"],
                        worker["host"],
                        worker["local_directory"],
                        worker["memory_limit"] / 2**30,
                    )
                    self.directories.add(location)
            totals = {
                key: sum(worker[key] for worker in workers) / 2**30
                for key in (
                    "rss",
                    "managed",
                    "unmanaged",
                    "unmanaged_recent",
                    "spill_disk",
                    "spill_memory",
                )
            }
            logger.info(
                "Dask memory heartbeat: workers=%d processing=%d RSS=%.2f GiB managed=%.2f GiB "
                "unmanaged=%.2f GiB unmanaged_recent=%.2f GiB spill_disk=%.2f GiB "
                "spill_memory=%.2f GiB (current retained spill, not cumulative I/O)",
                len(workers),
                sum(worker["processing"] for worker in workers),
                totals["rss"],
                totals["managed"],
                totals["unmanaged"],
                totals["unmanaged_recent"],
                totals["spill_disk"],
                totals["spill_memory"],
            )
            if workers:
                worst = max(workers, key=lambda worker: worker["rss"] / (worker["memory_limit"] or 1))
                logger.info(
                    "Dask highest RSS/limit: worker=%s status=%s RSS=%.2f GiB limit=%.2f GiB "
                    "managed=%.2f GiB unmanaged=%.2f GiB spill_disk=%.2f GiB",
                    worst["address"],
                    worst["status"],
                    worst["rss"] / 2**30,
                    worst["memory_limit"] / 2**30,
                    worst["managed"] / 2**30,
                    worst["unmanaged"] / 2**30,
                    worst["spill_disk"] / 2**30,
                )
        except Exception as exc:
            # Observability must never abort an otherwise healthy catalog build.
            logger.warning("Dask diagnostics unavailable; pipeline continues: %s", exc)


@contextmanager
def cluster_diagnostics(client, interval_seconds=DEFAULT_DIAGNOSTICS_INTERVAL_SECONDS):
    """Sample at startup and every interval; zero explicitly disables diagnostics."""
    if interval_seconds == 0:
        yield
        return
    diagnostics = _ClusterDiagnostics(client)
    stopped = threading.Event()
    diagnostics.report()

    def monitor():
        while not stopped.wait(interval_seconds):
            diagnostics.report()

    thread = threading.Thread(target=monitor, name="dask-memory-diagnostics", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        # A failed diagnostic RPC cannot hold up shutdown or client cleanup.
        thread.join(timeout=_DIAGNOSTICS_TIMEOUT_SECONDS + 1)
