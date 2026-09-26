"""trade-gateway: the broker gateway of futrisk (01 §5.3.3, 02 §6.4, 03 §4.2).

* :mod:`~futrisk.services.trade_gateway.main` - ``run()`` for ``futrisk svc trade-gateway``;
* :mod:`~futrisk.services.trade_gateway.service` - sync-runtime shell and business loop;
* :mod:`~futrisk.services.trade_gateway.core` - routing checks, tombstones, execution
  reports, resync, kill switch, queries, digests, trade import;
* :mod:`~futrisk.services.trade_gateway.broker` - ``BrokerAdapter`` interface,
  ``SimBrokerAdapter`` and the ``CtpBrokerAdapter`` placeholder;
* :mod:`~futrisk.services.trade_gateway.schema` / ``store`` - td.db tables and access.
"""
