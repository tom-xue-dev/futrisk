"""Daily risk report, statements and settlement JSON (01 §5.3.4 Reports, 03 §5.3).

* :mod:`.gather` - async data collection in the service process (clearing.db +
  ``risk.query.*`` / ``md.events`` / ``svc.history`` with graceful degradation)
  and registration of the produced files (``reports`` + ``report.ready``);
* :mod:`.render` - process-pool entry: writes every file of a trading day;
* :mod:`.model` - pure analytics behind the report sections;
* :mod:`.charts` - matplotlib (Agg, object API) charts as base64 PNG;
* :mod:`.statement` - CTP-style text and CSV statements.
"""

from futrisk.services.clearing.reports.sections import SECTION_TITLES, STATEMENT_HEADINGS

__all__ = ["SECTION_TITLES", "STATEMENT_HEADINGS"]
