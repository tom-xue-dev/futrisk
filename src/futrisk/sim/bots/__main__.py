"""``python -m futrisk.sim.bots``: run the strategy bots by hand.

Examples::

    python -m futrisk.sim.bots --home var
    python -m futrisk.sim.bots --home var --aggressive --accounts ACC3   # breaker demo (01 §6.3)
    python -m futrisk.sim.bots --home var --shard 1 --seed 7

The supervisor starts the same code with ``futrisk svc strategy-sim`` (01 §7.1).
"""

from __future__ import annotations

import argparse
import sys

from futrisk.sim.bots.main import run


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m futrisk.sim.bots", description="futrisk strategy bots (strategy-sim)")
    p.add_argument("--home", default=None, help="runtime home directory (default: FUTRISK_HOME or <repo>/var)")
    p.add_argument("--instance", default=None, help="instance name for the log file (default strategy-sim)")
    p.add_argument("--shard", type=int, default=None, help="only the accounts of this risk-engine shard")
    p.add_argument("--parent-pid", type=int, default=None, help="exit when this process dies")
    p.add_argument("--aggressive", action="store_true", default=None,
                   help="scenario mode: ACC3 builds its whole long book at once (breaker demo)")
    p.add_argument("--accounts", default=None, help="comma-separated account ids (default: all)")
    p.add_argument("--config", default=None, help="bots.yaml override file")
    p.add_argument("--seed", type=int, default=None, help="RNG seed (default from bots.yaml)")
    p.add_argument("--log-level", default=None, help="log level (default: app.yaml logging.level)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return run(
        home=args.home,
        instance=args.instance,
        shard=args.shard,
        parent_pid=args.parent_pid,
        aggressive=args.aggressive,
        accounts=args.accounts,
        config_path=args.config,
        seed=args.seed,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    sys.exit(main())
