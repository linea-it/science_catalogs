"""Dask executor helpers."""

import logging
from typing import Any

import dask
from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster

DEFAULT_DASK_CONNECT_TIMEOUT = "120s"
DEFAULT_DASK_TCP_TIMEOUT = "300s"


def get_executor(executor_cfg: dict[str, Any]):
    """Create a Dask cluster (local or Slurm) from the ``cluster`` YAML block."""
    logger = logging.getLogger(__name__)
    name = executor_cfg.get("executor", "local")
    connect_timeout = executor_cfg.get("dask_connect_timeout", DEFAULT_DASK_CONNECT_TIMEOUT)
    tcp_timeout = executor_cfg.get("dask_tcp_timeout", DEFAULT_DASK_TCP_TIMEOUT)
    dask.config.set(
        {
            "distributed.comm.timeouts.connect": connect_timeout,
            "distributed.comm.timeouts.tcp": tcp_timeout,
        }
    )
    logger.info(
        "Configured Dask communication timeouts: connect=%s, tcp=%s",
        connect_timeout,
        tcp_timeout,
    )

    if name == "local":
        args = dict(executor_cfg.get("local", {}))
        args.setdefault("n_workers", 1)
        args.setdefault("threads_per_worker", 1)
        args.setdefault("processes", True)
        args.setdefault("dashboard_address", None)
        logger.info("Creating LocalCluster with %s", args)
        cluster = LocalCluster(**args)
        return cluster

    if name == "slurm":
        args = dict(executor_cfg.get("slurm", {}))
        required = ("cores", "processes", "memory", "walltime")
        missing = [key for key in required if args.get(key) in (None, "")]
        if missing:
            raise ValueError("SLURM execution requires explicit values for: " + ", ".join(missing))
        scheduler_options = dict(args.get("scheduler_options", {}) or {})
        scheduler_options.setdefault("dashboard_address", args.get("dashboard_address"))
        job_extra_directives = args.get("job_extra_directives", []) or []

        cluster = SLURMCluster(
            interface=args.get("interface"),
            queue=args.get("queue"),
            cores=args.get("cores"),
            processes=args.get("processes"),
            memory=args.get("memory"),
            walltime=args.get("walltime"),
            death_timeout=args.get("death_timeout", 600),
            scheduler_options=scheduler_options,
            job_extra_directives=job_extra_directives,
        )
        scale = int(args.get("dask_scale_number", 1) or 1)
        cluster.scale(jobs=scale)
        return cluster

    raise ValueError(f"Executor '{name}' not supported")


__all__ = ["get_executor"]
