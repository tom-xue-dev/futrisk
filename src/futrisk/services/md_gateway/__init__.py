"""md-gateway: multi-source market data ingestion, arbitration and recording (01 §5.3.2, §8.13).

Pipeline (one asyncio loop): sources (Feed A WebSocket, Feed B REST, optional Sina) →
normalizer → validator → arbiter → ``md.tick.{instrument}`` (LIVE only) plus raw tick
recording, 1-minute bars, settlement inputs, ``md.status`` and ``md.event``.

Entry point: :func:`futrisk.services.md_gateway.main.run` (``futrisk svc md-gateway``).
"""
