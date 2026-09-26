"""Operator / tools commands of the ``futrisk`` CLI (01 §7.1; plan §9 "CLI"), registered by
:func:`register` from :data:`futrisk.main.TOPLEVEL`.

==============================================  ===========================================
command                                         module
==============================================  ===========================================
``positions``, ``accounts``, ``order ...``,     :mod:`futrisk.cli.trading_cmd`
``risk ...``
``settle ...``, ``report ...``                  :mod:`futrisk.cli.eod_cmd`
``fault ...``, ``shock``, ``scenario ...``,     :mod:`futrisk.cli.democtl_cmd`
``clock ...``
``doctor``                                      :mod:`futrisk.cli.doctor_cmd`
==============================================  ===========================================

Shared options, the API session and output live in :mod:`futrisk.cli.toolkit`, the HTTP
clients in :mod:`futrisk.cli.apiclient`. The command modules import only typer and the
standard library at import time (this module is loaded on every ``futrisk`` invocation).
"""

from __future__ import annotations

import typer

from futrisk.cli import democtl_cmd, doctor_cmd, eod_cmd, trading_cmd


def register(app: typer.Typer) -> None:
    """Add the tools commands to the ``futrisk`` CLI (``futrisk.main``)."""
    app.command("positions")(trading_cmd.positions)
    app.command("accounts")(trading_cmd.accounts)
    app.add_typer(trading_cmd.order_app, name="order")
    app.add_typer(trading_cmd.risk_app, name="risk")
    app.add_typer(eod_cmd.settle_app, name="settle")
    app.add_typer(eod_cmd.report_app, name="report")
    app.add_typer(democtl_cmd.fault_app, name="fault")
    app.command("shock")(democtl_cmd.shock)
    app.add_typer(democtl_cmd.scenario_app, name="scenario")
    app.add_typer(democtl_cmd.clock_app, name="clock")
    app.command("doctor")(doctor_cmd.doctor)


__all__ = ["register"]
