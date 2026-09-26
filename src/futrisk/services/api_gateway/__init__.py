"""api-gateway: REST + WebSocket, RBAC, CQRS read model, command routing, notifications,
config 2PC coordinator, demo orchestration and dashboard hosting (01 §5.3.5; 02 §4.1, §6.7, §8.5, §8.8, §9.6-§9.8).

Modules:

* :mod:`.main` - ``futrisk svc api-gateway`` entry point (``run``);
* :mod:`.service` - wiring onto :class:`~futrisk.runtime.AsyncServiceRuntime`;
* :mod:`.app` / :mod:`.routes` - FastAPI app (``/api/v1``, ``/ws``, ``/``);
* :mod:`.gateway` - dependency container, REST idempotency;
* :mod:`.auth` - API keys, bootstrap key, permission matrix;
* :mod:`.readmodel` / :mod:`.views` - read model, compare view, position aggregation;
* :mod:`.repair` - read-model FACT-gap repair and LV-silence watchdog;
* :mod:`.killswitch` - the persisted manual kill-switch desired state, fan-out and retries;
* :mod:`.router` - per-account shard routing, fan-out and read deadlines;
* :mod:`.wshub` - WebSocket protocol;
* :mod:`.notifier` - webhook notifications;
* :mod:`.config2pc` - config two-phase commit coordinator;
* :mod:`.clockjump` - market clock jump orchestration;
* :mod:`.schema` / :mod:`.store` - api.db.
"""
